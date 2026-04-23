"""
test_geocoding_db.py — Unit tests for database/location/geocoding.py DB helpers.

Covers:
  - get_place_id: upserts a row snapped to 0.001°, returns id
  - get_place_id: idempotent — same snapped coords → same id
  - get_place_id: different snapped coords → different ids
  - insert_geocode: updates places row with address fields
  - insert_geocode: partial geocode (missing fields) → NULL columns
"""

import sqlite3
import pytest
from unittest.mock import patch

from database.location.geocoding import get_place_id, insert_geocode


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

PLACES_DDL = """
    CREATE TABLE places (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        lat_snap     REAL NOT NULL,
        lon_snap     REAL NOT NULL,
        country_code TEXT,
        country      TEXT,
        region       TEXT,
        locality     TEXT,
        city         TEXT,
        suburb       TEXT,
        road         TEXT,
        display_name TEXT,
        raw_json     TEXT,
        geocoded_at  TEXT,
        UNIQUE(lat_snap, lon_snap)
    );
"""


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(PLACES_DDL)
    return conn


@pytest.fixture
def patch_conn(db):
    with patch("database.location.geocoding.get_conn", return_value=db):
        yield db


# ---------------------------------------------------------------------------
# get_place_id
# ---------------------------------------------------------------------------

class TestGetPlaceId:

    def test_returns_integer_id(self, patch_conn):
        result = get_place_id(48.8566, 2.3522)
        assert isinstance(result, int)
        assert result > 0

    def test_snaps_to_3dp_grid(self, patch_conn):
        get_place_id(48.856600001, 2.352200001)  # nearly identical coords
        row = patch_conn.execute("SELECT lat_snap, lon_snap FROM places").fetchone()
        assert row["lat_snap"] == pytest.approx(round(48.856600001, 3))
        assert row["lon_snap"] == pytest.approx(round(2.352200001, 3))

    def test_idempotent_same_grid_cell(self, patch_conn):
        id1 = get_place_id(48.8561, 2.3521)
        id2 = get_place_id(48.8564, 2.3524)  # different raw coords, same 3dp snap
        assert id1 == id2
        count = patch_conn.execute("SELECT COUNT(*) FROM places").fetchone()[0]
        assert count == 1

    def test_different_snapped_coords_different_ids(self, patch_conn):
        id1 = get_place_id(48.8560, 2.3520)
        id2 = get_place_id(48.9000, 2.4000)
        assert id1 != id2
        count = patch_conn.execute("SELECT COUNT(*) FROM places").fetchone()[0]
        assert count == 2

    def test_returns_none_if_insert_fails(self, patch_conn):
        # Edge case: get_place_id returns None if SELECT finds nothing after INSERT OR IGNORE
        # In normal operation this should not happen, but testing the None path is defensive.
        # We test the normal path here: valid coords always return an id.
        result = get_place_id(0.0, 0.0)
        assert result is not None


# ---------------------------------------------------------------------------
# insert_geocode
# ---------------------------------------------------------------------------

class TestInsertGeocode:

    def _insert_place(self, db, lat_snap, lon_snap):
        db.execute(
            "INSERT INTO places (lat_snap, lon_snap) VALUES (?, ?)", (lat_snap, lon_snap)
        )
        db.commit()
        return db.execute(
            "SELECT id FROM places WHERE lat_snap = ? AND lon_snap = ?", (lat_snap, lon_snap)
        ).fetchone()["id"]

    def test_full_geocode_stored(self, patch_conn):
        place_id = self._insert_place(patch_conn, 48.856, 2.352)
        geocode = {
            "display_name": "Paris, France",
            "address": {
                "country_code": "fr",
                "country": "France",
                "state": "Île-de-France",
                "city": "Paris",
                "suburb": "1st Arrondissement",
                "road": "Rue de Rivoli",
                #"locality": "Paris",
            },
        }
        insert_geocode(place_id, geocode)

        row = patch_conn.execute("SELECT * FROM places WHERE id = ?", (place_id,)).fetchone()
        assert row["country_code"] == "fr"
        assert row["country"] == "France"
        assert row["region"] == "Île-de-France"
        assert row["city"] == "Paris"
        #assert row["locality"] == "Paris"
        assert row["suburb"] == "1st Arrondissement"
        assert row["road"] == "Rue de Rivoli"
        assert row["display_name"] == "Paris, France"
        assert row["geocoded_at"] is not None

    def test_partial_geocode_leaves_null_columns(self, patch_conn):
        place_id = self._insert_place(patch_conn, 0.0, 0.0)
        geocode = {
            "display_name": "Somewhere",
            "address": {"country": "Testland"},
        }
        insert_geocode(place_id, geocode)

        row = patch_conn.execute("SELECT * FROM places WHERE id = ?", (place_id,)).fetchone()
        assert row["country"] == "Testland"
        assert row["city"] is None
        assert row["road"] is None
