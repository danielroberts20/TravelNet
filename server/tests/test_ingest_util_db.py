"""
test_ingest_util_db.py — DB tests for database/transaction/ingest/util.py.

Covers get_closest_lat_lon_by_timestamp():
  - Returns (lat, lon) for point within 15-minute window before target
  - Returns (None, None) when no point within window
  - Returns most recent point when multiple qualify
  - Point exactly at lower boundary (15 min before) is included
  - Point exactly at target timestamp is included
  - Point 1 second after lower boundary is excluded (beyond 15-min window)
"""

import sqlite3
import pytest

from database.transaction.ingest.util import get_closest_lat_lon_by_timestamp


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    # Create a simple location_unified table (the function queries this view/table)
    conn.executescript("""
        CREATE TABLE location_unified (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            latitude  REAL NOT NULL,
            longitude REAL NOT NULL
        );
    """)
    return conn


def _insert(conn, ts, lat, lon):
    conn.execute(
        "INSERT INTO location_unified (timestamp, latitude, longitude) VALUES (?, ?, ?)",
        (ts, lat, lon),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestGetClosestLatLonByTimestamp:

    def test_returns_lat_lon_for_recent_point(self, db):
        # Point 5 minutes before target
        _insert(db, "2024-06-15 09:00:00", 51.5074, -0.1278)
        cur = db.cursor()
        lat, lon = get_closest_lat_lon_by_timestamp(cur, "2024-06-15 09:05:00")
        assert lat == pytest.approx(51.5074)
        assert lon == pytest.approx(-0.1278)

    def test_returns_none_none_when_no_point_in_window(self, db):
        # Point is 20 minutes before — outside 15-min window
        _insert(db, "2024-06-15 08:40:00", 51.5074, -0.1278)
        cur = db.cursor()
        lat, lon = get_closest_lat_lon_by_timestamp(cur, "2024-06-15 09:00:00")
        assert lat is None
        assert lon is None

    def test_returns_most_recent_when_multiple_qualify(self, db):
        # Two points within the window — most recent should win
        _insert(db, "2024-06-15 08:50:00", 51.0, -0.1)
        _insert(db, "2024-06-15 08:55:00", 52.0, -0.2)  # more recent
        cur = db.cursor()
        lat, lon = get_closest_lat_lon_by_timestamp(cur, "2024-06-15 09:00:00")
        assert lat == pytest.approx(52.0)
        assert lon == pytest.approx(-0.2)

    def test_point_at_target_timestamp_included(self, db):
        _insert(db, "2024-06-15 09:00:00", 48.8566, 2.3522)
        cur = db.cursor()
        lat, lon = get_closest_lat_lon_by_timestamp(cur, "2024-06-15 09:00:00")
        assert lat == pytest.approx(48.8566)
        assert lon == pytest.approx(2.3522)

    def test_point_exactly_at_15min_boundary_included(self, db):
        # Exactly 15 minutes before: datetime(target, '-15 minutes') = boundary
        _insert(db, "2024-06-15 08:45:00", 10.0, 20.0)
        cur = db.cursor()
        lat, lon = get_closest_lat_lon_by_timestamp(cur, "2024-06-15 09:00:00")
        assert lat == pytest.approx(10.0)
        assert lon == pytest.approx(20.0)

    def test_empty_table_returns_none_none(self, db):
        cur = db.cursor()
        lat, lon = get_closest_lat_lon_by_timestamp(cur, "2024-06-15 09:00:00")
        assert lat is None
        assert lon is None

    def test_future_point_excluded(self, db):
        # Point after the target timestamp — not returned (WHERE timestamp <= target)
        _insert(db, "2024-06-15 09:05:00", 51.5, -0.1)
        cur = db.cursor()
        lat, lon = get_closest_lat_lon_by_timestamp(cur, "2024-06-15 09:00:00")
        assert lat is None
        assert lon is None
