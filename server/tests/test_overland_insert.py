"""
test_overland_insert.py — Unit tests for database/location/overland/table.py.

Covers insert_payload():
  - Returns (inserted, skipped) counts
  - Idempotency: same point twice → (1, 1) across two calls
  - GeoJSON coordinate order: coordinates[0]=lon, coordinates[1]=lat
  - place_id snapped to 0.001° grid and assigned from places table
  - High horizontal_accuracy (> threshold) → row inserted into noise table
  - Low horizontal_accuracy (≤ threshold) → not in noise table
  - Empty payload → (0, 0)
"""

import json
import sqlite3
import pytest
from unittest.mock import patch

from database.location.overland.table import LocationOverlandTable
from models.telemetry import OverlandPayload


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript("""
        CREATE TABLE places (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            lat_snap     REAL NOT NULL,
            lon_snap     REAL NOT NULL,
            country_code TEXT,
            country      TEXT,
            region       TEXT,
            city         TEXT,
            suburb       TEXT,
            road         TEXT,
            display_name TEXT,
            geocoded_at  TEXT,
            UNIQUE(lat_snap, lon_snap)
        );

        CREATE TABLE location_overland (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id           TEXT NOT NULL,
            timestamp           TEXT NOT NULL,
            latitude            REAL NOT NULL,
            longitude           REAL NOT NULL,
            altitude            REAL,
            speed               REAL,
            horizontal_accuracy REAL,
            vertical_accuracy   REAL,
            motion              TEXT,
            activity            TEXT,
            wifi_ssid           TEXT,
            battery_state       TEXT,
            battery_level       REAL,
            pauses              INTEGER,
            desired_accuracy    REAL,
            significant_change  TEXT,
            place_id            INTEGER REFERENCES places(id),
            raw_json            TEXT,
            inserted_at         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
            UNIQUE(device_id, timestamp)
        );

        CREATE TABLE location_noise (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            overland_id INTEGER NOT NULL REFERENCES location_overland(id) ON DELETE CASCADE,
            tier        INTEGER NOT NULL,
            reason      TEXT NOT NULL,
            flagged_at  TEXT NOT NULL DEFAULT(strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
        );

        CREATE UNIQUE INDEX idx_noise_overland_id ON location_noise(overland_id);
    """)
    return conn


@pytest.fixture
def tbl(db):
    with patch("database.location.overland.table.get_conn", return_value=db), \
         patch("database.location.noise.table.get_conn", return_value=db):
        yield LocationOverlandTable(), db


def _make_feature(lon=2.3522, lat=48.8566, ts="2024-06-15T09:00:00+00:00",
                  speed=1.0, h_acc=10.0, v_acc=3.0):
    return {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [lon, lat]},
        "properties": {
            "timestamp": ts,
            "speed": speed,
            "horizontal_accuracy": h_acc,
            "vertical_accuracy": v_acc,
        },
    }


def _make_payload(features):
    return OverlandPayload(locations=features)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestInsertPayload:

    def test_single_point_inserted(self, tbl):
        t, db = tbl
        payload = _make_payload([_make_feature()])
        inserted, skipped = t.insert_payload(payload, "iphone")
        assert inserted == 1
        assert skipped == 0

    def test_empty_payload(self, tbl):
        t, db = tbl
        payload = _make_payload([])
        inserted, skipped = t.insert_payload(payload, "iphone")
        assert inserted == 0
        assert skipped == 0

    def test_duplicate_point_skipped(self, tbl):
        t, db = tbl
        feature = _make_feature()
        payload = _make_payload([feature])
        i1, s1 = t.insert_payload(payload, "iphone")
        i2, s2 = t.insert_payload(payload, "iphone")
        assert i1 == 1 and s1 == 0
        assert i2 == 0 and s2 == 1

    def test_coordinate_order_lon_lat(self, tbl):
        """GeoJSON [lon, lat] — verify DB stores them in correct columns."""
        t, db = tbl
        lon, lat = 2.3522, 48.8566
        payload = _make_payload([_make_feature(lon=lon, lat=lat)])
        t.insert_payload(payload, "iphone")
        row = db.execute("SELECT latitude, longitude FROM location_overland LIMIT 1").fetchone()
        assert row["latitude"] == pytest.approx(lat)
        assert row["longitude"] == pytest.approx(lon)

    def test_place_id_assigned(self, tbl):
        """place_id is assigned from the places table using grid-snapped coords."""
        t, db = tbl
        payload = _make_payload([_make_feature(lon=2.3522, lat=48.8566)])
        t.insert_payload(payload, "iphone")

        row = db.execute("SELECT place_id FROM location_overland LIMIT 1").fetchone()
        assert row["place_id"] is not None

        place = db.execute(
            "SELECT lat_snap, lon_snap FROM places WHERE id = ?", (row["place_id"],)
        ).fetchone()
        # Snapped to 3dp
        assert place["lat_snap"] == pytest.approx(round(48.8566, 3))
        assert place["lon_snap"] == pytest.approx(round(2.3522, 3))

    def test_same_snapped_coords_reuse_place_id(self, tbl):
        """Two nearby points that snap to the same grid cell share one places row."""
        t, db = tbl
        payload = _make_payload([
            _make_feature(lon=2.3521, lat=48.8561, ts="2024-06-15T09:00:00+00:00"),
            _make_feature(lon=2.3524, lat=48.8564, ts="2024-06-15T10:00:00+00:00"),
        ])
        t.insert_payload(payload, "iphone")

        place_count = db.execute("SELECT COUNT(*) FROM places").fetchone()[0]
        assert place_count == 1  # both snap to 48.856, 2.352

    def test_high_accuracy_point_goes_to_noise(self, tbl):
        """horizontal_accuracy > THRESHOLD (100) → inserted into location_noise tier 1."""
        t, db = tbl
        payload = _make_payload([_make_feature(h_acc=150.0)])  # > 100
        t.insert_payload(payload, "iphone")

        overland_id = db.execute("SELECT id FROM location_overland LIMIT 1").fetchone()["id"]
        noise_row = db.execute(
            "SELECT tier, reason FROM location_noise WHERE overland_id = ?", (overland_id,)
        ).fetchone()
        assert noise_row is not None
        assert noise_row["tier"] == 1
        assert noise_row["reason"] == "accuracy_threshold"

    def test_low_accuracy_point_not_in_noise(self, tbl):
        """horizontal_accuracy ≤ THRESHOLD → not in noise table."""
        t, db = tbl
        payload = _make_payload([_make_feature(h_acc=50.0)])  # ≤ 100
        t.insert_payload(payload, "iphone")

        count = db.execute("SELECT COUNT(*) FROM location_noise").fetchone()[0]
        assert count == 0

    def test_null_accuracy_not_in_noise(self, tbl):
        """No horizontal_accuracy → noise check skipped, no noise row."""
        t, db = tbl
        payload = _make_payload([_make_feature(h_acc=None)])
        t.insert_payload(payload, "iphone")

        count = db.execute("SELECT COUNT(*) FROM location_noise").fetchone()[0]
        assert count == 0

    def test_multiple_points_inserted(self, tbl):
        t, db = tbl
        features = [
            _make_feature(ts="2024-06-15T09:00:00+00:00"),
            _make_feature(ts="2024-06-15T10:00:00+00:00"),
            _make_feature(ts="2024-06-15T11:00:00+00:00"),
        ]
        payload = _make_payload(features)
        inserted, skipped = t.insert_payload(payload, "iphone")
        assert inserted == 3
        assert skipped == 0
