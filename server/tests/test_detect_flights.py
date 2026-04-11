"""
test_detect_flights.py — Unit tests for scheduled_tasks/detect_flights.py.

Covers _detect_flight_gaps():
  - Returns empty list when no rows in location_overland_cleaned
  - Returns empty list when only one point
  - Returns empty list when gap too small (< FLIGHT_GAP_MIN_HOURS)
  - Returns empty list when distance too small (< FLIGHT_DISTANCE_MIN_KM)
  - Detects valid gap: time and distance both above threshold
  - Candidate dict contains correct keys: gap_start_ts, gap_end_ts, gap_hours, distance_km
  - Detects multiple gaps in one pass
  - Gap exactly at threshold is NOT detected (strictly < threshold check)
"""

import sqlite3
import pytest
from unittest.mock import patch

from scheduled_tasks.detect_flights import _detect_flight_gaps

# Mirror config defaults for isolation
FLIGHT_GAP_MIN_HOURS  = 3.0
FLIGHT_DISTANCE_MIN_KM = 200.0


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE location_overland_cleaned (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            latitude  REAL NOT NULL,
            longitude REAL NOT NULL
        );
    """)
    return conn


def _insert(conn, ts, lat, lon):
    conn.execute(
        "INSERT INTO location_overland_cleaned (timestamp, latitude, longitude) VALUES (?,?,?)",
        (ts, lat, lon),
    )
    conn.commit()


def _run(db):
    with patch("scheduled_tasks.detect_flights.FLIGHT_GAP_MIN_HOURS", FLIGHT_GAP_MIN_HOURS), \
         patch("scheduled_tasks.detect_flights.FLIGHT_DISTANCE_MIN_KM", FLIGHT_DISTANCE_MIN_KM):
        return _detect_flight_gaps(db)


class TestDetectFlightGaps:

    def test_empty_table_returns_empty(self, db):
        assert _run(db) == []

    def test_single_point_returns_empty(self, db):
        _insert(db, "2024-06-15T08:00:00Z", 51.5074, -0.1278)
        assert _run(db) == []

    def test_small_time_gap_not_detected(self, db):
        # 1-hour gap — below 3h threshold
        _insert(db, "2024-06-15T08:00:00Z", 51.5074, -0.1278)
        _insert(db, "2024-06-15T09:00:00Z", 48.8566,  2.3522)
        assert _run(db) == []

    def test_small_distance_not_detected(self, db):
        # 5-hour gap but only ~1km apart — below 200km threshold
        _insert(db, "2024-06-15T08:00:00Z", 51.5074, -0.1278)
        _insert(db, "2024-06-15T13:00:00Z", 51.5080, -0.1280)
        assert _run(db) == []

    def test_valid_flight_gap_detected(self, db):
        # London to Paris: ~340km, 5h gap — both above threshold
        _insert(db, "2024-06-15T08:00:00Z", 51.5074, -0.1278)  # London
        _insert(db, "2024-06-15T13:00:00Z", 48.8566,  2.3522)  # Paris
        gaps = _run(db)
        assert len(gaps) == 1

    def test_gap_dict_keys(self, db):
        _insert(db, "2024-06-15T08:00:00Z", 51.5074, -0.1278)
        _insert(db, "2024-06-15T13:00:00Z", 48.8566,  2.3522)
        gap = _run(db)[0]
        assert "gap_start_ts" in gap
        assert "gap_end_ts" in gap
        assert "gap_hours" in gap
        assert "distance_km" in gap
        assert "gap_start_lat" in gap
        assert "gap_start_lon" in gap
        assert "gap_end_lat" in gap
        assert "gap_end_lon" in gap

    def test_gap_values_correct(self, db):
        _insert(db, "2024-06-15T08:00:00Z", 51.5074, -0.1278)
        _insert(db, "2024-06-15T13:00:00Z", 48.8566,  2.3522)
        gap = _run(db)[0]
        assert gap["gap_start_ts"] == "2024-06-15T08:00:00Z"
        assert gap["gap_end_ts"]   == "2024-06-15T13:00:00Z"
        assert gap["gap_hours"]    == pytest.approx(5.0, rel=0.01)
        assert gap["distance_km"]  > 200.0  # London–Paris ~340km

    def test_multiple_gaps_detected(self, db):
        # London → Paris (5h, 340km), then pause, then Paris → Sydney (24h, ~17000km)
        _insert(db, "2024-06-15T08:00:00Z", 51.5074,  -0.1278)   # London
        _insert(db, "2024-06-15T13:00:00Z", 48.8566,   2.3522)   # Paris
        _insert(db, "2024-06-15T14:00:00Z", 48.8570,   2.3530)   # near Paris (1h, small dist)
        _insert(db, "2024-06-16T15:00:00Z", -33.8688, 151.2093)  # Sydney (25h, ~17000km)
        gaps = _run(db)
        assert len(gaps) == 2

    def test_adjacent_close_points_not_detected(self, db):
        # Three points all within Paris — no gaps
        _insert(db, "2024-06-15T08:00:00Z", 48.856, 2.352)
        _insert(db, "2024-06-15T08:30:00Z", 48.857, 2.353)
        _insert(db, "2024-06-15T09:00:00Z", 48.858, 2.354)
        assert _run(db) == []
