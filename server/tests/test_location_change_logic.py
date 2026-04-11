"""
test_location_change_logic.py — Unit tests for triggers/location_change.py pure helpers.

Covers:
  - compute_centroid: < min points → None
  - compute_centroid: any point outside stationarity radius → None
  - compute_centroid: all points within radius → (avg_lat, avg_lon, earliest_ts)
  - compute_centroid: single point at minimum count
  - get_nearest_known_place: returns closest within LOCATION_CHANGE_RADIUS_M
  - get_nearest_known_place: returns None when all places are outside radius
  - get_nearest_known_place: returns closest when multiple qualify
"""

import sqlite3
import pytest
from unittest.mock import patch

from triggers.location_change import compute_centroid, get_nearest_known_place


# Defaults from config.general — mirror them here so tests don't depend on live config
STATIONARITY_RADIUS_M = 50   # default LOCATION_STATIONARITY_RADIUS_M
MINIMUM_POINTS        = 3    # default LOCATION_MINIMUM_POINTS
CHANGE_RADIUS_M       = 100  # default LOCATION_CHANGE_RADIUS_M


def _make_points(*coords_ts):
    """Build a list of dicts from (lat, lon, timestamp) triples."""
    return [{"latitude": lat, "longitude": lon, "timestamp": ts} for lat, lon, ts in coords_ts]


# ---------------------------------------------------------------------------
# compute_centroid
# ---------------------------------------------------------------------------

class TestComputeCentroid:

    def test_too_few_points_returns_none(self):
        points = _make_points(
            (51.5074, -0.1278, "2024-06-15T09:00:00Z"),
            (51.5074, -0.1278, "2024-06-15T09:01:00Z"),
        )
        with patch("triggers.location_change.LOCATION_MINIMUM_POINTS", MINIMUM_POINTS), \
             patch("triggers.location_change.LOCATION_STATIONARITY_RADIUS_M", STATIONARITY_RADIUS_M):
            result = compute_centroid(points)
        assert result is None

    def test_empty_list_returns_none(self):
        with patch("triggers.location_change.LOCATION_MINIMUM_POINTS", MINIMUM_POINTS), \
             patch("triggers.location_change.LOCATION_STATIONARITY_RADIUS_M", STATIONARITY_RADIUS_M):
            result = compute_centroid([])
        assert result is None

    def test_point_outside_radius_returns_none(self):
        # First three points close together, but the fourth is far away
        points = _make_points(
            (51.5074, -0.1278, "2024-06-15T09:00:00Z"),
            (51.5074, -0.1279, "2024-06-15T09:01:00Z"),
            (51.5074, -0.1277, "2024-06-15T09:02:00Z"),
            (51.6000, -0.2000, "2024-06-15T09:03:00Z"),  # ~10km away
        )
        with patch("triggers.location_change.LOCATION_MINIMUM_POINTS", MINIMUM_POINTS), \
             patch("triggers.location_change.LOCATION_STATIONARITY_RADIUS_M", STATIONARITY_RADIUS_M):
            result = compute_centroid(points)
        assert result is None

    def test_all_same_point_returns_that_point(self):
        points = _make_points(
            (51.5074, -0.1278, "2024-06-15T08:00:00Z"),
            (51.5074, -0.1278, "2024-06-15T09:00:00Z"),
            (51.5074, -0.1278, "2024-06-15T10:00:00Z"),
        )
        with patch("triggers.location_change.LOCATION_MINIMUM_POINTS", MINIMUM_POINTS), \
             patch("triggers.location_change.LOCATION_STATIONARITY_RADIUS_M", STATIONARITY_RADIUS_M):
            result = compute_centroid(points)
        assert result is not None
        avg_lat, avg_lon, earliest = result
        assert avg_lat == pytest.approx(51.5074)
        assert avg_lon == pytest.approx(-0.1278)
        assert earliest == "2024-06-15T08:00:00Z"

    def test_returns_average_of_close_points(self):
        # Three points very close together (within stationarity radius)
        points = _make_points(
            (51.5074, -0.1278, "2024-06-15T08:00:00Z"),
            (51.5075, -0.1277, "2024-06-15T09:00:00Z"),
            (51.5076, -0.1276, "2024-06-15T10:00:00Z"),
        )
        with patch("triggers.location_change.LOCATION_MINIMUM_POINTS", MINIMUM_POINTS), \
             patch("triggers.location_change.LOCATION_STATIONARITY_RADIUS_M", STATIONARITY_RADIUS_M):
            result = compute_centroid(points)
        assert result is not None
        avg_lat, avg_lon, earliest = result
        assert avg_lat == pytest.approx((51.5074 + 51.5075 + 51.5076) / 3)
        assert avg_lon == pytest.approx((-0.1278 + -0.1277 + -0.1276) / 3)

    def test_earliest_timestamp_returned(self):
        # Points in non-chronological order — earliest should still be found
        points = _make_points(
            (51.5074, -0.1278, "2024-06-15T10:00:00Z"),
            (51.5074, -0.1278, "2024-06-15T08:00:00Z"),  # earliest
            (51.5074, -0.1278, "2024-06-15T09:00:00Z"),
        )
        with patch("triggers.location_change.LOCATION_MINIMUM_POINTS", MINIMUM_POINTS), \
             patch("triggers.location_change.LOCATION_STATIONARITY_RADIUS_M", STATIONARITY_RADIUS_M):
            result = compute_centroid(points)
        assert result is not None
        _, _, earliest = result
        assert earliest == "2024-06-15T08:00:00Z"

    def test_exactly_minimum_points(self):
        points = _make_points(
            (51.5074, -0.1278, "2024-06-15T09:00:00Z"),
            (51.5074, -0.1278, "2024-06-15T09:01:00Z"),
            (51.5074, -0.1278, "2024-06-15T09:02:00Z"),
        )
        with patch("triggers.location_change.LOCATION_MINIMUM_POINTS", MINIMUM_POINTS), \
             patch("triggers.location_change.LOCATION_STATIONARITY_RADIUS_M", STATIONARITY_RADIUS_M):
            result = compute_centroid(points)
        assert result is not None


# ---------------------------------------------------------------------------
# get_nearest_known_place
# ---------------------------------------------------------------------------

class TestGetNearestKnownPlace:

    @pytest.fixture
    def db(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript("""
            CREATE TABLE known_places (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                latitude  REAL NOT NULL,
                longitude REAL NOT NULL,
                first_seen TEXT NOT NULL
            );
        """)
        return conn

    def _insert_place(self, db, lat, lon):
        db.execute(
            "INSERT INTO known_places (latitude, longitude, first_seen) VALUES (?, ?, ?)",
            (lat, lon, "2024-01-01T00:00:00Z"),
        )
        db.commit()
        return db.execute("SELECT last_insert_rowid()").fetchone()[0]

    def test_returns_nearest_within_radius(self, db):
        # Place essentially at the same point
        place_id = self._insert_place(db, 51.5074, -0.1278)
        with patch("triggers.location_change.get_conn", return_value=db), \
             patch("triggers.location_change.LOCATION_CHANGE_RADIUS_M", CHANGE_RADIUS_M):
            result = get_nearest_known_place(51.5074, -0.1278)
        assert result is not None
        assert result["id"] == place_id

    def test_returns_none_when_all_outside_radius(self, db):
        self._insert_place(db, 48.8566, 2.3522)  # Paris (~340km from London)
        with patch("triggers.location_change.get_conn", return_value=db), \
             patch("triggers.location_change.LOCATION_CHANGE_RADIUS_M", CHANGE_RADIUS_M):
            result = get_nearest_known_place(51.5074, -0.1278)  # London
        assert result is None

    def test_returns_closest_when_multiple_qualify(self, db):
        # Two nearby places — the closer one should be returned
        close_id = self._insert_place(db, 51.50740, -0.12780)  # ~0m
        self._insert_place(db, 51.50800, -0.12800)             # ~70m away
        with patch("triggers.location_change.get_conn", return_value=db), \
             patch("triggers.location_change.LOCATION_CHANGE_RADIUS_M", CHANGE_RADIUS_M):
            result = get_nearest_known_place(51.50740, -0.12780)
        assert result is not None
        assert result["id"] == close_id

    def test_empty_table_returns_none(self, db):
        with patch("triggers.location_change.get_conn", return_value=db), \
             patch("triggers.location_change.LOCATION_CHANGE_RADIUS_M", CHANGE_RADIUS_M):
            result = get_nearest_known_place(51.5074, -0.1278)
        assert result is None
