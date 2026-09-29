"""
test_public_location.py — Unit tests for public/location.py.

Covers get_latest_location():
  - no rows → None
  - returns fuzzed coordinates that differ from the raw DB value
  - last_synced is formatted as an ISO 8601 UTC string

Covers get_location_history():
  - no rows → []
  - returns points ordered by timestamp, all fuzzed vs. raw
  - downsampling drops points that are neither far-enough-apart in time
    nor a meaningful move, but always keeps the first and last point
  - a meaningful move within the downsample window is still kept

Covers _fuzz():
  - jitter distance is within [0, PUBLIC_LOCATION_FUZZ_RADIUS_KM] (with margin)
  - two calls on the same input produce different output (random, not fixed offset)
"""

import sqlite3
import pytest
from unittest.mock import patch

from public.location import get_latest_location, get_location_history, _fuzz
from util import haversine_km

RAW_LAT, RAW_LON = 51.5074, -0.1278  # London

LOCATION_UNIFIED_DDL = """
    CREATE TABLE location_unified (
        timestamp TEXT NOT NULL,
        latitude  REAL NOT NULL,
        longitude REAL NOT NULL
    );
"""


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(LOCATION_UNIFIED_DDL)
    return conn


@pytest.fixture
def patch_conn(db):
    with patch("public.location.get_conn", return_value=db):
        yield db


# ---------------------------------------------------------------------------
# get_latest_location
# ---------------------------------------------------------------------------

class TestGetLatestLocation:

    def test_no_rows_returns_none(self, patch_conn):
        assert get_latest_location() is None

    def test_returns_fuzzed_coordinates_not_raw(self, patch_conn):
        patch_conn.execute(
            "INSERT INTO location_unified VALUES (?, ?, ?)",
            ("2026-04-11T12:00:00Z", RAW_LAT, RAW_LON),
        )
        result = get_latest_location()
        assert result is not None
        assert (result["latitude"], result["longitude"]) != (RAW_LAT, RAW_LON)

    def test_fuzzed_point_within_expected_radius(self, patch_conn):
        patch_conn.execute(
            "INSERT INTO location_unified VALUES (?, ?, ?)",
            ("2026-04-11T12:00:00Z", RAW_LAT, RAW_LON),
        )
        result = get_latest_location()
        dist = haversine_km(RAW_LAT, RAW_LON, result["latitude"], result["longitude"])
        # Generous upper bound — exact radius is a runtime-editable config value
        assert 0 < dist <= 20

    def test_last_synced_formatted(self, patch_conn):
        patch_conn.execute(
            "INSERT INTO location_unified VALUES (?, ?, ?)",
            ("2026-04-11T12:00:00Z", RAW_LAT, RAW_LON),
        )
        result = get_latest_location()
        assert result["last_synced"] == "2026-04-11T12:00:00Z"

    def test_returns_most_recent_of_multiple_rows(self, patch_conn):
        patch_conn.execute(
            "INSERT INTO location_unified VALUES (?, ?, ?)",
            ("2026-04-11T10:00:00Z", 10.0, 10.0),
        )
        patch_conn.execute(
            "INSERT INTO location_unified VALUES (?, ?, ?)",
            ("2026-04-11T12:00:00Z", RAW_LAT, RAW_LON),
        )
        result = get_latest_location()
        assert result["last_synced"] == "2026-04-11T12:00:00Z"


# ---------------------------------------------------------------------------
# get_location_history
# ---------------------------------------------------------------------------

class TestGetLocationHistory:

    def test_no_rows_returns_empty_list(self, patch_conn):
        from datetime import datetime, timezone
        assert get_location_history(datetime(2026, 1, 1, tzinfo=timezone.utc)) == []

    def test_points_are_fuzzed_not_raw(self, patch_conn):
        from datetime import datetime, timezone
        patch_conn.execute(
            "INSERT INTO location_unified VALUES (?, ?, ?)",
            ("2026-04-11T12:00:00Z", RAW_LAT, RAW_LON),
        )
        result = get_location_history(datetime(2026, 1, 1, tzinfo=timezone.utc))
        assert len(result) == 1
        assert (result[0]["latitude"], result[0]["longitude"]) != (RAW_LAT, RAW_LON)

    def test_ordered_by_timestamp_ascending(self, patch_conn):
        from datetime import datetime, timezone
        patch_conn.execute(
            "INSERT INTO location_unified VALUES (?, ?, ?)",
            ("2026-04-11T14:00:00Z", RAW_LAT, RAW_LON),
        )
        patch_conn.execute(
            "INSERT INTO location_unified VALUES (?, ?, ?)",
            ("2026-04-11T12:00:00Z", RAW_LAT, RAW_LON),
        )
        result = get_location_history(datetime(2026, 1, 1, tzinfo=timezone.utc))
        timestamps = [p["timestamp"] for p in result]
        assert timestamps == sorted(timestamps)

    def test_downsamples_dense_stationary_points(self, patch_conn):
        """Points a minute apart with no meaningful movement collapse down,
        but the very first and last are always kept."""
        from datetime import datetime, timezone
        # 20 points, 1 minute apart, same location (well under the
        # downsample-minutes threshold and under LOCATION_CHANGE_RADIUS_M).
        for i in range(20):
            patch_conn.execute(
                "INSERT INTO location_unified VALUES (?, ?, ?)",
                (f"2026-04-11T12:{i:02d}:00Z", RAW_LAT, RAW_LON),
            )
        result = get_location_history(datetime(2026, 1, 1, tzinfo=timezone.utc))
        assert len(result) < 20
        assert result[0]["timestamp"] == "2026-04-11T12:00:00Z"
        assert result[-1]["timestamp"] == "2026-04-11T12:19:00Z"

    def test_meaningful_move_is_kept_even_within_downsample_window(self, patch_conn):
        """A large jump (~50km) one minute later must survive downsampling
        even though it's well under the time threshold."""
        from datetime import datetime, timezone
        patch_conn.execute(
            "INSERT INTO location_unified VALUES (?, ?, ?)",
            ("2026-04-11T12:00:00Z", RAW_LAT, RAW_LON),
        )
        patch_conn.execute(
            "INSERT INTO location_unified VALUES (?, ?, ?)",
            ("2026-04-11T12:01:00Z", RAW_LAT + 0.5, RAW_LON + 0.5),  # ~50km+ away
        )
        result = get_location_history(datetime(2026, 1, 1, tzinfo=timezone.utc))
        assert len(result) == 2


# ---------------------------------------------------------------------------
# _fuzz
# ---------------------------------------------------------------------------

class TestFuzz:

    def test_fuzz_changes_coordinates(self):
        lat, lon = _fuzz(RAW_LAT, RAW_LON)
        assert (lat, lon) != (RAW_LAT, RAW_LON)

    def test_fuzz_is_randomised_across_calls(self):
        results = {_fuzz(RAW_LAT, RAW_LON) for _ in range(10)}
        # Extremely unlikely to collide 10 times in a row if truly randomised
        assert len(results) > 1

    def test_fuzz_stays_within_generous_bound(self):
        for _ in range(20):
            lat, lon = _fuzz(RAW_LAT, RAW_LON)
            dist = haversine_km(RAW_LAT, RAW_LON, lat, lon)
            assert 0 < dist <= 20
