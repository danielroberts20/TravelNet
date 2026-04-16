"""
test_location_change_logic.py — Comprehensive tests for triggers/location_change.py.

Replaces the previous file that covered the now-removed compute_centroid function.

Covers:
  get_stationary_streak      — streak detection, anchoring, centroid, duration
  _streak_duration_mins      — pure timestamp duration helper
  get_nearest_known_place    — nearest known place within radius
  get_all_open_visits        — all open (undeparted) place_visits rows
  get_first_point_after      — first GPS point strictly after a timestamp
  get_last_in_radius_timestamp — most recent in-radius GPS point since visit start
  visit_exists               — idempotency guard used by retroactive scanner
  check_departure            — close confirmed-departed visits (all open, accurate timestamp)
  detect_arrival             — unified arrival handler (known place + new place)
  _handle_known_place        — record a return visit or note ongoing one
  _handle_new_place          — promote streak to known place + fire notification
  run                        — end-to-end real-time trigger
"""

import sqlite3
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock

from triggers.location_change import (
    get_stationary_streak,
    _streak_duration_mins,
    get_nearest_known_place,
    get_all_open_visits,
    get_first_point_after,
    get_last_in_radius_timestamp,
    visit_exists,
    check_departure,
    detect_arrival,
    _handle_known_place,
    _handle_new_place,
    run,
)


# ---------------------------------------------------------------------------
# Config constants used throughout — tests patch these explicitly
# ---------------------------------------------------------------------------

MIN_POINTS   = 3      # LOCATION_MINIMUM_POINTS
STATION_M    = 150    # LOCATION_STATIONARITY_RADIUS_M
CHANGE_M     = 500    # LOCATION_CHANGE_RADIUS_M
STAY_MINS    = 30     # LOCATION_STAY_DURATION_MINS
REVISIT_MINS = 5      # LOCATION_REVISIT_DURATION_MINS
DEPART_MINS  = 5      # LOCATION_DEPARTURE_CONFIRMATION_MINS
POINT_LIMIT  = 1000   # LOCATION_STREAK_POINT_LIMIT

# Reference locations
#   LONDON        — base location
#   LONDON_NEAR   — ~12 m away, within STATION_M (150 m) — same cluster
#   LONDON_MID    — ~270 m away, within CHANGE_M (500 m) but outside STATION_M
#   PARIS         — ~340 km away, well outside CHANGE_M
LONDON      = (51.5074, -0.1278)
LONDON_NEAR = (51.5075, -0.1279)
LONDON_MID  = (51.5077, -0.1245)
PARIS       = (48.8566,  2.3522)


# ---------------------------------------------------------------------------
# Timestamp helpers
# ---------------------------------------------------------------------------

def _now(offset_mins: int = 0) -> str:
    """Return an ISO UTC string `offset_mins` minutes relative to now (negative = past)."""
    dt = datetime.now(timezone.utc) + timedelta(minutes=offset_mins)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _fixed(offset_mins: int = 0) -> str:
    """Return a fixed historical ISO UTC timestamp + offset_mins."""
    base = datetime(2024, 6, 15, 9, 0, 0, tzinfo=timezone.utc)
    return (base + timedelta(minutes=offset_mins)).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# Shared DB fixture and helpers
# ---------------------------------------------------------------------------

@pytest.fixture
def db():
    """In-memory SQLite with all tables used by location_change.py."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE location_unified (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            latitude  REAL NOT NULL,
            longitude REAL NOT NULL
        );
        CREATE TABLE known_places (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            latitude         REAL NOT NULL,
            longitude        REAL NOT NULL,
            first_seen       TEXT NOT NULL,
            visit_count      INTEGER NOT NULL DEFAULT 0,
            last_visited     TEXT,
            total_time_mins  INTEGER NOT NULL DEFAULT 0,
            current_visit_id INTEGER,
            label            TEXT
        );
        CREATE TABLE place_visits (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            place_id      INTEGER NOT NULL,
            arrived_at    TEXT NOT NULL,
            departed_at   TEXT,
            duration_mins INTEGER
        );
        CREATE TABLE places (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            lat_snap     REAL,
            lon_snap     REAL,
            geocoded_at  TEXT,
            country_code TEXT, country TEXT, region TEXT,
            city TEXT, suburb TEXT, road TEXT, display_name TEXT
        );
    """)
    return conn


def _pt(db, lat, lon, ts):
    """Insert a single location_unified point."""
    db.execute(
        "INSERT INTO location_unified (timestamp, latitude, longitude) VALUES (?,?,?)",
        (ts, lat, lon),
    )
    db.commit()


def _place(db, lat, lon, first_seen=None, label=None, visit_count=1):
    """Insert a known_places row and return its id."""
    first_seen = first_seen or _fixed()
    cur = db.execute(
        """INSERT INTO known_places
               (latitude, longitude, first_seen, visit_count, last_visited, label)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (lat, lon, first_seen, visit_count, first_seen, label),
    )
    db.commit()
    return cur.lastrowid


def _visit(db, place_id, arrived_at, departed_at=None, duration_mins=None):
    """Insert a place_visits row and return its id."""
    cur = db.execute(
        "INSERT INTO place_visits (place_id, arrived_at, departed_at, duration_mins) VALUES (?,?,?,?)",
        (place_id, arrived_at, departed_at, duration_mins),
    )
    db.commit()
    return cur.lastrowid


def _set_cv(db, place_id, visit_id):
    """Set current_visit_id on a known_places row."""
    db.execute(
        "UPDATE known_places SET current_visit_id = ? WHERE id = ?",
        (visit_id, place_id),
    )
    db.commit()


# Config patch helper — apply common overrides in one call
def _cfg(**overrides):
    """Return a list of patch context managers for config constants."""
    defaults = dict(
        LOCATION_MINIMUM_POINTS=MIN_POINTS,
        LOCATION_STATIONARITY_RADIUS_M=STATION_M,
        LOCATION_CHANGE_RADIUS_M=CHANGE_M,
        LOCATION_STAY_DURATION_MINS=STAY_MINS,
        LOCATION_REVISIT_DURATION_MINS=REVISIT_MINS,
        LOCATION_DEPARTURE_CONFIRMATION_MINS=DEPART_MINS,
        LOCATION_STREAK_POINT_LIMIT=POINT_LIMIT,
    )
    defaults.update(overrides)
    return [
        patch(f"triggers.location_change.{k}", v)
        for k, v in defaults.items()
    ]


# ===========================================================================
# get_stationary_streak
# ===========================================================================

class TestGetStationaryStreak:

    def _run(self, db, **cfg_overrides):
        patches = _cfg(**cfg_overrides)
        with patch("triggers.location_change.get_conn", return_value=db):
            ctx = __import__("contextlib").ExitStack()
            for p in patches:
                ctx.enter_context(p)
            with ctx:
                return get_stationary_streak()

    def test_empty_db_returns_none(self, db):
        assert self._run(db) is None

    def test_too_few_points_returns_none(self, db):
        lat, lon = LONDON
        for i in range(MIN_POINTS - 1):
            _pt(db, lat, lon, _fixed(i))
        assert self._run(db) is None

    def test_exactly_minimum_points_returns_streak(self, db):
        lat, lon = LONDON
        for i in range(MIN_POINTS):
            _pt(db, lat, lon, _fixed(i))
        assert self._run(db) is not None

    def test_all_same_point_returns_streak(self, db):
        lat, lon = LONDON
        for i in range(5):
            _pt(db, lat, lon, _fixed(i))
        result = self._run(db)
        assert result is not None

    def test_returns_five_tuple(self, db):
        lat, lon = LONDON
        for i in range(5):
            _pt(db, lat, lon, _fixed(i))
        result = self._run(db)
        assert len(result) == 5

    def test_centroid_is_mean_of_streak_points(self, db):
        lats = [51.5074, 51.5075, 51.5076]
        lons = [-0.1278, -0.1279, -0.1277]
        for i, (la, lo) in enumerate(zip(lats, lons)):
            _pt(db, la, lo, _fixed(i))
        result = self._run(db)
        assert result is not None
        avg_lat, avg_lon, _, _, _ = result
        assert avg_lat == pytest.approx(sum(lats) / len(lats))
        assert avg_lon == pytest.approx(sum(lons) / len(lons))

    def test_streak_end_is_most_recent_point(self, db):
        lat, lon = LONDON
        for i in range(5):
            _pt(db, lat, lon, _fixed(i))
        _, _, streak_start, streak_end, _ = self._run(db)
        assert streak_end == _fixed(4)

    def test_streak_start_is_oldest_point(self, db):
        lat, lon = LONDON
        for i in range(5):
            _pt(db, lat, lon, _fixed(i))
        _, _, streak_start, streak_end, _ = self._run(db)
        assert streak_start == _fixed(0)

    def test_point_count_is_streak_length(self, db):
        lat, lon = LONDON
        for i in range(7):
            _pt(db, lat, lon, _fixed(i))
        _, _, _, _, count = self._run(db)
        assert count == 7

    def test_movement_breaks_streak(self, db):
        lat, lon = LONDON
        far_lat, far_lon = PARIS
        # Insert 5 stationary points at LONDON
        for i in range(5):
            _pt(db, lat, lon, _fixed(i))
        # Insert an older point at PARIS — since DB returns DESC, this older
        # point would appear at the end of the list; it breaks the streak
        _pt(db, far_lat, far_lon, _fixed(-10))
        result = self._run(db)
        # Streak stops at PARIS point — only 5 LONDON points in streak
        assert result is not None
        _, _, _, _, count = result
        assert count == 5

    def test_recent_movement_makes_streak_too_short(self, db):
        # One stationary point, then movement — streak only has 1 point → None
        lat, lon = LONDON
        far_lat, far_lon = PARIS
        _pt(db, far_lat, far_lon, _fixed(-10))  # older, outside radius
        _pt(db, lat, lon, _fixed(0))            # most recent, stationary
        # Only 1 point in streak (MIN_POINTS=3) → None
        assert self._run(db) is None

    def test_point_limit_respected(self, db):
        lat, lon = LONDON
        for i in range(20):
            _pt(db, lat, lon, _fixed(i))
        # With limit=5, only the 5 most recent are queried
        result = self._run(db, LOCATION_STREAK_POINT_LIMIT=5)
        assert result is not None
        _, _, _, _, count = result
        assert count == 5

    def test_gps_drift_within_radius_included(self, db):
        # Points that drift up to ~12 m — all within 150 m STATION_M
        lat, lon = LONDON
        near_lat, near_lon = LONDON_NEAR
        _pt(db, lat,      lon,      _fixed(0))
        _pt(db, near_lat, near_lon, _fixed(1))
        _pt(db, lat,      lon,      _fixed(2))
        result = self._run(db)
        assert result is not None
        _, _, _, _, count = result
        assert count == 3

    def test_point_outside_stationarity_radius_breaks_streak(self, db):
        lat, lon = LONDON
        mid_lat, mid_lon = LONDON_MID   # ~270 m — outside STATION_M
        # Most recent is LONDON, older is MID
        _pt(db, mid_lat, mid_lon, _fixed(-5))  # older, outside radius
        _pt(db, lat,     lon,     _fixed(0))   # most recent
        _pt(db, lat,     lon,     _fixed(1))   # most recent
        _pt(db, lat,     lon,     _fixed(2))   # most recent
        # Streak = 3 LONDON points, MID point breaks it
        result = self._run(db)
        assert result is not None
        _, _, _, _, count = result
        assert count == 3


# ===========================================================================
# _streak_duration_mins
# ===========================================================================

class TestStreakDurationMins:

    def test_30_minute_streak(self):
        start = _fixed(0)
        end   = _fixed(30)
        assert _streak_duration_mins(start, end) == pytest.approx(30.0)

    def test_zero_duration_same_timestamp(self):
        ts = _fixed(0)
        assert _streak_duration_mins(ts, ts) == pytest.approx(0.0)

    def test_multi_hour_streak(self):
        start = _fixed(0)
        end   = _fixed(120)
        assert _streak_duration_mins(start, end) == pytest.approx(120.0)

    def test_returns_float(self):
        result = _streak_duration_mins(_fixed(0), _fixed(1))
        assert isinstance(result, float)

    def test_sub_minute_precision(self):
        # 90 seconds = 1.5 minutes
        base = datetime(2024, 6, 15, 9, 0, 0, tzinfo=timezone.utc)
        start = base.strftime("%Y-%m-%dT%H:%M:%SZ")
        end   = (base + timedelta(seconds=90)).strftime("%Y-%m-%dT%H:%M:%SZ")
        assert _streak_duration_mins(start, end) == pytest.approx(1.5)


# ===========================================================================
# get_nearest_known_place
# ===========================================================================

class TestGetNearestKnownPlace:

    def _run(self, db, lat, lon):
        with patch("triggers.location_change.get_conn", return_value=db), \
             patch("triggers.location_change.LOCATION_CHANGE_RADIUS_M", CHANGE_M):
            return get_nearest_known_place(lat, lon)

    def test_returns_nearest_within_radius(self, db):
        lat, lon = LONDON
        place_id = _place(db, lat, lon)
        result = self._run(db, lat, lon)
        assert result is not None
        assert result["id"] == place_id

    def test_returns_none_when_all_outside_radius(self, db):
        _place(db, *PARIS)
        result = self._run(db, *LONDON)
        assert result is None

    def test_returns_closest_when_multiple_qualify(self, db):
        close_id = _place(db, *LONDON)       # ~0 m
        _place(db, *LONDON_MID)              # ~270 m — also within 500 m
        result = self._run(db, *LONDON)
        assert result is not None
        assert result["id"] == close_id

    def test_empty_table_returns_none(self, db):
        result = self._run(db, *LONDON)
        assert result is None

    def test_place_just_inside_radius_returned(self, db):
        # LONDON_MID is ~270 m — within 500 m change radius
        place_id = _place(db, *LONDON_MID)
        result = self._run(db, *LONDON)
        assert result is not None
        assert result["id"] == place_id

    def test_place_beyond_radius_not_returned(self, db):
        _place(db, *LONDON_MID)
        # Use a 100 m change radius — LONDON_MID (~270 m) is outside
        with patch("triggers.location_change.get_conn", return_value=db), \
             patch("triggers.location_change.LOCATION_CHANGE_RADIUS_M", 100):
            result = get_nearest_known_place(*LONDON)
        assert result is None


# ===========================================================================
# get_all_open_visits
# ===========================================================================

class TestGetAllOpenVisits:

    def _run(self, db):
        with patch("triggers.location_change.get_conn", return_value=db):
            return get_all_open_visits()

    def test_empty_db_returns_empty(self, db):
        assert self._run(db) == []

    def test_returns_open_visits_only(self, db):
        place_id = _place(db, *LONDON)
        open_id   = _visit(db, place_id, _fixed(0))
        closed_id = _visit(db, place_id, _fixed(-60), departed_at=_fixed(-30), duration_mins=30)
        results = self._run(db)
        ids = [r["id"] for r in results]
        assert open_id in ids
        assert closed_id not in ids

    def test_returns_all_open_visits(self, db):
        place_a = _place(db, *LONDON)
        place_b = _place(db, *LONDON_MID)
        _visit(db, place_a, _fixed(0))
        _visit(db, place_b, _fixed(-10))
        results = self._run(db)
        assert len(results) == 2

    def test_row_contains_place_coordinates(self, db):
        lat, lon = LONDON
        place_id = _place(db, lat, lon)
        _visit(db, place_id, _fixed(0))
        results = self._run(db)
        assert len(results) == 1
        assert results[0]["latitude"]  == pytest.approx(lat)
        assert results[0]["longitude"] == pytest.approx(lon)

    def test_ordered_by_arrived_at_desc(self, db):
        place_id = _place(db, *LONDON)
        earlier_id = _visit(db, place_id, _fixed(0))
        later_id   = _visit(db, place_id, _fixed(30))
        results = self._run(db)
        assert results[0]["id"] == later_id   # most recent first
        assert results[1]["id"] == earlier_id


# ===========================================================================
# get_first_point_after
# ===========================================================================

class TestGetFirstPointAfter:

    def _run(self, db, ts):
        with patch("triggers.location_change.get_conn", return_value=db):
            return get_first_point_after(ts)

    def test_returns_first_point_after_timestamp(self, db):
        lat, lon = LONDON
        _pt(db, lat, lon, _fixed(0))
        _pt(db, lat, lon, _fixed(10))
        _pt(db, lat, lon, _fixed(20))
        row = self._run(db, _fixed(5))
        assert row is not None
        assert row["timestamp"] == _fixed(10)

    def test_returns_none_when_no_points_after(self, db):
        _pt(db, *LONDON, _fixed(0))
        assert self._run(db, _fixed(10)) is None

    def test_excludes_point_at_exact_timestamp(self, db):
        _pt(db, *LONDON, _fixed(0))
        # Exactly at _fixed(0) should NOT be returned when querying _fixed(0)
        assert self._run(db, _fixed(0)) is None

    def test_empty_db_returns_none(self, db):
        assert self._run(db, _fixed(0)) is None

    def test_returns_earliest_of_multiple_subsequent(self, db):
        _pt(db, *LONDON, _fixed(10))
        _pt(db, *LONDON, _fixed(20))
        _pt(db, *LONDON, _fixed(30))
        row = self._run(db, _fixed(5))
        assert row["timestamp"] == _fixed(10)


# ===========================================================================
# get_last_in_radius_timestamp
# ===========================================================================

class TestGetLastInRadiusTimestamp:

    def _run(self, db, kp_lat, kp_lon, arrived_at):
        with patch("triggers.location_change.get_conn", return_value=db), \
             patch("triggers.location_change.LOCATION_CHANGE_RADIUS_M", CHANGE_M):
            return get_last_in_radius_timestamp(kp_lat, kp_lon, arrived_at)

    def test_returns_most_recent_in_radius_point(self, db):
        lat, lon = LONDON
        _pt(db, lat, lon, _fixed(0))
        _pt(db, lat, lon, _fixed(10))
        _pt(db, lat, lon, _fixed(20))
        result = self._run(db, lat, lon, _fixed(-5))
        assert result == _fixed(20)

    def test_returns_none_when_all_outside_radius(self, db):
        _pt(db, *PARIS, _fixed(0))
        _pt(db, *PARIS, _fixed(10))
        result = self._run(db, *LONDON, _fixed(-5))
        assert result is None

    def test_ignores_points_before_arrived_at(self, db):
        lat, lon = LONDON
        _pt(db, lat, lon, _fixed(-30))  # before arrived_at
        _pt(db, lat, lon, _fixed(10))   # after arrived_at
        result = self._run(db, lat, lon, _fixed(0))
        assert result == _fixed(10)

    def test_returns_none_for_empty_table(self, db):
        result = self._run(db, *LONDON, _fixed(0))
        assert result is None

    def test_ignores_out_of_radius_between_valid_points(self, db):
        lat, lon = LONDON
        _pt(db, lat,    lon,    _fixed(0))
        _pt(db, *PARIS, _fixed(10))    # out of radius — should be skipped
        _pt(db, lat,    lon,    _fixed(20))
        result = self._run(db, lat, lon, _fixed(-5))
        assert result == _fixed(20)

    def test_point_at_edge_of_radius_included(self, db):
        lat, lon = LONDON
        # LONDON_MID is ~270 m, within CHANGE_M 500 m
        _pt(db, *LONDON_MID, _fixed(0))
        result = self._run(db, lat, lon, _fixed(-5))
        assert result == _fixed(0)


# ===========================================================================
# visit_exists
# ===========================================================================

class TestVisitExists:

    def _run(self, db, place_id, arrived_at, tolerance=5):
        with patch("triggers.location_change.get_conn", return_value=db):
            return visit_exists(place_id, arrived_at, tolerance)

    def test_no_visits_returns_false(self, db):
        _place(db, *LONDON)
        assert self._run(db, 1, _fixed(0)) is False

    def test_exact_match_returns_true(self, db):
        place_id = _place(db, *LONDON)
        _visit(db, place_id, _fixed(0))
        assert self._run(db, place_id, _fixed(0)) is True

    def test_within_tolerance_returns_true(self, db):
        place_id = _place(db, *LONDON)
        _visit(db, place_id, _fixed(0))
        # Query 3 minutes later — within default tolerance of 5
        assert self._run(db, place_id, _fixed(3)) is True

    def test_outside_tolerance_returns_false(self, db):
        place_id = _place(db, *LONDON)
        _visit(db, place_id, _fixed(0))
        # Query 10 minutes later — outside tolerance of 5
        assert self._run(db, place_id, _fixed(10)) is False

    def test_different_place_id_returns_false(self, db):
        place_a = _place(db, *LONDON)
        place_b = _place(db, *LONDON_MID)
        _visit(db, place_a, _fixed(0))
        # Visit exists for place_a, not place_b
        assert self._run(db, place_b, _fixed(0)) is False

    def test_closed_visit_still_detected(self, db):
        place_id = _place(db, *LONDON)
        _visit(db, place_id, _fixed(0), departed_at=_fixed(30), duration_mins=30)
        assert self._run(db, place_id, _fixed(0)) is True


# ===========================================================================
# check_departure
# ===========================================================================

class TestCheckDeparture:
    """check_departure iterates all open visits and closes any that are confirmed departed."""

    def _run(self, db):
        with patch("triggers.location_change.get_conn", return_value=db), \
             patch("database.location.known_places.table.get_conn", return_value=db), \
             patch("triggers.location_change.LOCATION_CHANGE_RADIUS_M", CHANGE_M), \
             patch("triggers.location_change.LOCATION_DEPARTURE_CONFIRMATION_MINS", DEPART_MINS):
            check_departure()

    def test_no_open_visits_does_nothing(self, db):
        place_id = _place(db, *LONDON)
        _visit(db, place_id, _fixed(0), departed_at=_fixed(30), duration_mins=30)
        self._run(db)  # should not raise or modify anything
        row = db.execute("SELECT departed_at FROM place_visits").fetchone()
        assert row["departed_at"] == _fixed(30)

    def test_recent_in_radius_point_not_closed(self, db):
        """Last in-radius point is within confirmation window → no departure."""
        lat, lon = LONDON
        place_id = _place(db, lat, lon)
        visit_id = _visit(db, place_id, _fixed(-60))
        _set_cv(db, place_id, visit_id)
        # In-radius point 2 mins ago — within DEPART_MINS=5 window
        _pt(db, lat, lon, _now(-2))
        self._run(db)
        row = db.execute("SELECT departed_at FROM place_visits WHERE id = ?", (visit_id,)).fetchone()
        assert row["departed_at"] is None

    def test_old_in_radius_point_with_subsequent_point_closes_visit(self, db):
        """Last in-radius point is old AND subsequent out-of-radius point exists → close visit."""
        lat, lon = LONDON
        place_id = _place(db, lat, lon)
        visit_id = _visit(db, place_id, _fixed(0))
        _set_cv(db, place_id, visit_id)
        _pt(db, lat, lon, _fixed(10))        # last in-radius
        _pt(db, *PARIS, _fixed(20))          # subsequent out-of-radius
        self._run(db)
        row = db.execute("SELECT departed_at FROM place_visits WHERE id = ?", (visit_id,)).fetchone()
        assert row["departed_at"] is not None

    def test_departed_at_equals_last_in_radius_not_now(self, db):
        """departed_at must be the last in-radius timestamp, not the current time."""
        lat, lon = LONDON
        place_id = _place(db, lat, lon)
        visit_id = _visit(db, place_id, _fixed(0))
        _set_cv(db, place_id, visit_id)
        last_in = _fixed(15)
        _pt(db, lat, lon, last_in)
        _pt(db, *PARIS, _fixed(20))
        self._run(db)
        row = db.execute("SELECT departed_at FROM place_visits WHERE id = ?", (visit_id,)).fetchone()
        assert row["departed_at"] == last_in

    def test_duration_mins_computed_correctly(self, db):
        lat, lon = LONDON
        place_id = _place(db, lat, lon)
        arrived   = _fixed(0)
        last_in   = _fixed(45)   # 45 min stay
        visit_id  = _visit(db, place_id, arrived)
        _set_cv(db, place_id, visit_id)
        _pt(db, lat, lon, last_in)
        _pt(db, *PARIS, _fixed(50))
        self._run(db)
        row = db.execute("SELECT duration_mins FROM place_visits WHERE id = ?", (visit_id,)).fetchone()
        assert row["duration_mins"] == 45

    def test_no_point_after_last_in_does_not_close(self, db):
        """If tracking stops after the last in-radius point, we can't confirm departure."""
        lat, lon = LONDON
        place_id = _place(db, lat, lon)
        visit_id = _visit(db, place_id, _fixed(0))
        _set_cv(db, place_id, visit_id)
        _pt(db, lat, lon, _fixed(10))  # last in-radius, no subsequent points at all
        self._run(db)
        row = db.execute("SELECT departed_at FROM place_visits WHERE id = ?", (visit_id,)).fetchone()
        assert row["departed_at"] is None

    def test_current_visit_id_cleared_on_close(self, db):
        lat, lon = LONDON
        place_id = _place(db, lat, lon)
        visit_id = _visit(db, place_id, _fixed(0))
        _set_cv(db, place_id, visit_id)
        _pt(db, lat, lon, _fixed(10))
        _pt(db, *PARIS, _fixed(20))
        self._run(db)
        row = db.execute("SELECT current_visit_id FROM known_places WHERE id = ?", (place_id,)).fetchone()
        assert row["current_visit_id"] is None

    def test_multiple_open_visits_all_processed(self, db):
        """All open visits are iterated — not just the first one."""
        lat_a, lon_a = LONDON
        lat_b, lon_b = LONDON_MID
        place_a = _place(db, lat_a, lon_a)
        place_b = _place(db, lat_b, lon_b)
        visit_a = _visit(db, place_a, _fixed(0))
        visit_b = _visit(db, place_b, _fixed(-30))
        _pt(db, lat_a, lon_a, _fixed(5))
        _pt(db, lat_b, lon_b, _fixed(-25))
        _pt(db, *PARIS, _fixed(60))   # subsequent out-of-radius for both
        self._run(db)
        rows = db.execute("SELECT departed_at FROM place_visits").fetchall()
        assert all(r["departed_at"] is not None for r in rows), \
            "Both visits should have been closed"

    def test_one_departed_one_still_active(self, db):
        """Only the departed visit is closed; the active one at a different place stays open.

        Uses two different known places so that the active visit's recent in-radius
        point does not satisfy the departed visit's radius check.
        """
        # Place A (LONDON) — visited and departed long ago
        place_a = _place(db, *LONDON)
        old_visit = _visit(db, place_a, _fixed(-120))
        _pt(db, *LONDON, _fixed(-90))    # last in-radius point for A
        _pt(db, *PARIS,  _fixed(-80))    # subsequent out-of-radius — confirms departure from A

        # Place B (PARIS) — currently active, in-radius 2 mins ago
        place_b = _place(db, *PARIS)
        active_visit = _visit(db, place_b, _fixed(-60))
        _pt(db, *PARIS, _now(-2))        # within confirmation window for B

        self._run(db)

        old_row    = db.execute("SELECT departed_at FROM place_visits WHERE id = ?", (old_visit,)).fetchone()
        active_row = db.execute("SELECT departed_at FROM place_visits WHERE id = ?", (active_visit,)).fetchone()
        assert old_row["departed_at"] is not None
        assert active_row["departed_at"] is None

    def test_orphaned_visit_no_in_radius_point_uses_arrived_at_fallback(self, db):
        """A visit with no in-radius GPS points uses arrived_at as fallback departed_at."""
        lat, lon = LONDON
        place_id = _place(db, lat, lon)
        arrived  = _fixed(0)
        visit_id = _visit(db, place_id, arrived)
        # Only out-of-radius points exist
        _pt(db, *PARIS, _fixed(10))
        self._run(db)
        row = db.execute("SELECT departed_at FROM place_visits WHERE id = ?", (visit_id,)).fetchone()
        assert row["departed_at"] == arrived

    def test_total_time_mins_accumulated(self, db):
        lat, lon = LONDON
        place_id = _place(db, lat, lon)
        visit_id = _visit(db, place_id, _fixed(0))
        _set_cv(db, place_id, visit_id)
        _pt(db, lat, lon, _fixed(60))
        _pt(db, *PARIS, _fixed(70))
        self._run(db)
        row = db.execute("SELECT total_time_mins FROM known_places WHERE id = ?", (place_id,)).fetchone()
        assert row["total_time_mins"] == 60


# ===========================================================================
# detect_arrival
# ===========================================================================

class TestDetectArrival:
    """detect_arrival calls get_stationary_streak then handles known/new place."""

    def _make_streak(self, lat, lon, start_offset=0, duration_mins=STAY_MINS, n=5):
        """Produce a synthetic streak tuple."""
        return (
            lat, lon,
            _fixed(start_offset),
            _fixed(start_offset + duration_mins),
            n,
        )

    def _run(self, db, streak, **extra_patches):
        patches = _cfg()
        with patch("triggers.location_change.get_conn", return_value=db), \
             patch("database.location.known_places.table.get_conn", return_value=db), \
             patch("triggers.location_change.get_stationary_streak", return_value=streak), \
             patch("triggers.location_change.dispatch"), \
             patch("triggers.location_change.get_address", return_value={"city": "London"}):
            ctx = __import__("contextlib").ExitStack()
            for p in patches:
                ctx.enter_context(p)
            with ctx:
                return detect_arrival(streak)

    def test_no_streak_returns_false(self, db):
        result = self._run(db, None)
        assert result is False

    def test_known_place_streak_below_revisit_threshold_returns_false(self, db):
        lat, lon = LONDON
        _place(db, lat, lon)
        # Duration 2 mins — below REVISIT_MINS=5
        streak = self._make_streak(lat, lon, duration_mins=2)
        result = self._run(db, streak)
        assert result is False

    def test_known_place_streak_at_revisit_threshold_opens_visit(self, db):
        lat, lon = LONDON
        place_id = _place(db, lat, lon)
        streak = self._make_streak(lat, lon, duration_mins=REVISIT_MINS)
        result = self._run(db, streak)
        assert result is True
        row = db.execute("SELECT id FROM place_visits WHERE place_id = ?", (place_id,)).fetchone()
        assert row is not None

    def test_known_place_already_visiting_no_duplicate(self, db):
        lat, lon = LONDON
        place_id = _place(db, lat, lon)
        visit_id = _visit(db, place_id, _fixed(0))
        _set_cv(db, place_id, visit_id)
        streak = self._make_streak(lat, lon, duration_mins=REVISIT_MINS)
        self._run(db, streak)
        count = db.execute("SELECT COUNT(*) FROM place_visits").fetchone()[0]
        assert count == 1  # no second visit created

    def test_unknown_place_below_stay_threshold_returns_false(self, db):
        lat, lon = LONDON
        # Duration 10 mins — below STAY_MINS=30
        streak = self._make_streak(lat, lon, duration_mins=10)
        result = self._run(db, streak)
        assert result is False

    def test_unknown_place_at_stay_threshold_creates_place(self, db):
        lat, lon = LONDON
        streak = self._make_streak(lat, lon, duration_mins=STAY_MINS)
        result = self._run(db, streak)
        assert result is True
        row = db.execute("SELECT id FROM known_places").fetchone()
        assert row is not None

    def test_unknown_place_creates_visit_with_streak_start_as_arrived_at(self, db):
        lat, lon = LONDON
        streak = self._make_streak(lat, lon, start_offset=0, duration_mins=STAY_MINS)
        self._run(db, streak)
        row = db.execute("SELECT arrived_at FROM place_visits").fetchone()
        assert row["arrived_at"] == _fixed(0)  # streak_start

    def test_known_place_between_revisit_and_stay_threshold_opens_visit(self, db):
        """Duration 10 min — too short for a new place (30) but enough for revisit (5)."""
        lat, lon = LONDON
        place_id = _place(db, lat, lon)
        streak = self._make_streak(lat, lon, duration_mins=10)
        result = self._run(db, streak)
        assert result is True


# ===========================================================================
# _handle_known_place
# ===========================================================================

class TestHandleKnownPlace:

    def _run(self, db, nearest, arrived_at):
        with patch("triggers.location_change.get_conn", return_value=db), \
             patch("database.location.known_places.table.get_conn", return_value=db):
            _handle_known_place(nearest, arrived_at)

    def _nearest(self, db, lat, lon):
        place_id = _place(db, lat, lon)
        return db.execute(
            "SELECT id, latitude, longitude FROM known_places WHERE id = ?", (place_id,)
        ).fetchone()

    def test_already_visiting_does_not_insert_visit(self, db):
        nearest = self._nearest(db, *LONDON)
        visit_id = _visit(db, nearest["id"], _fixed(0))
        _set_cv(db, nearest["id"], visit_id)
        self._run(db, nearest, _fixed(60))
        count = db.execute("SELECT COUNT(*) FROM place_visits").fetchone()[0]
        assert count == 1  # no second visit

    def test_already_visiting_does_not_change_current_visit_id(self, db):
        nearest = self._nearest(db, *LONDON)
        visit_id = _visit(db, nearest["id"], _fixed(0))
        _set_cv(db, nearest["id"], visit_id)
        self._run(db, nearest, _fixed(60))
        row = db.execute(
            "SELECT current_visit_id FROM known_places WHERE id = ?", (nearest["id"],)
        ).fetchone()
        assert row["current_visit_id"] == visit_id

    def test_not_visiting_inserts_new_visit(self, db):
        nearest = self._nearest(db, *LONDON)
        self._run(db, nearest, _fixed(0))
        row = db.execute("SELECT id FROM place_visits WHERE place_id = ?", (nearest["id"],)).fetchone()
        assert row is not None

    def test_not_visiting_sets_arrived_at(self, db):
        nearest = self._nearest(db, *LONDON)
        arrived = _fixed(15)
        self._run(db, nearest, arrived)
        row = db.execute("SELECT arrived_at FROM place_visits").fetchone()
        assert row["arrived_at"] == arrived

    def test_not_visiting_increments_visit_count(self, db):
        nearest = self._nearest(db, *LONDON)
        self._run(db, nearest, _fixed(0))
        row = db.execute(
            "SELECT visit_count FROM known_places WHERE id = ?", (nearest["id"],)
        ).fetchone()
        assert row["visit_count"] == 2  # was 1 after _place(), now 2

    def test_not_visiting_sets_current_visit_id(self, db):
        nearest = self._nearest(db, *LONDON)
        self._run(db, nearest, _fixed(0))
        kp = db.execute(
            "SELECT current_visit_id FROM known_places WHERE id = ?", (nearest["id"],)
        ).fetchone()
        pv = db.execute(
            "SELECT id FROM place_visits WHERE place_id = ?", (nearest["id"],)
        ).fetchone()
        assert kp["current_visit_id"] == pv["id"]


# ===========================================================================
# _handle_new_place
# ===========================================================================

class TestHandleNewPlace:

    def _run(self, db, lat, lon, arrived_at):
        with patch("triggers.location_change.get_conn", return_value=db), \
             patch("database.location.known_places.table.get_conn", return_value=db), \
             patch("triggers.location_change.dispatch") as mock_dispatch, \
             patch("triggers.location_change.get_address",
                   return_value={"city": "London", "suburb": None, "road": None,
                                 "region": None, "display_name": "London, UK"}):
            _handle_new_place(lat, lon, arrived_at)
            return mock_dispatch

    def test_creates_known_places_row(self, db):
        lat, lon = LONDON
        self._run(db, lat, lon, _fixed(0))
        row = db.execute("SELECT id FROM known_places").fetchone()
        assert row is not None

    def test_stores_correct_coordinates(self, db):
        lat, lon = LONDON
        self._run(db, lat, lon, _fixed(0))
        row = db.execute("SELECT latitude, longitude FROM known_places").fetchone()
        assert row["latitude"]  == pytest.approx(lat)
        assert row["longitude"] == pytest.approx(lon)

    def test_creates_place_visits_row(self, db):
        lat, lon = LONDON
        self._run(db, lat, lon, _fixed(0))
        row = db.execute("SELECT id FROM place_visits").fetchone()
        assert row is not None

    def test_arrived_at_stored_correctly(self, db):
        lat, lon = LONDON
        arrived = _fixed(10)
        self._run(db, lat, lon, arrived)
        row = db.execute("SELECT arrived_at FROM place_visits").fetchone()
        assert row["arrived_at"] == arrived

    def test_current_visit_id_set(self, db):
        lat, lon = LONDON
        self._run(db, lat, lon, _fixed(0))
        kp = db.execute("SELECT current_visit_id FROM known_places").fetchone()
        pv = db.execute("SELECT id FROM place_visits").fetchone()
        assert kp["current_visit_id"] == pv["id"]

    def test_dispatch_called(self, db):
        lat, lon = LONDON
        mock_dispatch = self._run(db, lat, lon, _fixed(0))
        mock_dispatch.assert_called_once()

    def test_dispatch_trigger_is_location_change(self, db):
        lat, lon = LONDON
        mock_dispatch = self._run(db, lat, lon, _fixed(0))
        call_kwargs = mock_dispatch.call_args
        assert call_kwargs.kwargs.get("trigger") == "location_change"


# ===========================================================================
# run (end-to-end real-time trigger)
# ===========================================================================

class TestRun:
    """Integration tests for run() — verifies check_departure then detect_arrival."""

    def _run(self, db, streak=None):
        with patch("triggers.location_change.get_conn", return_value=db), \
             patch("database.location.known_places.table.get_conn", return_value=db), \
             patch("triggers.location_change.get_stationary_streak", return_value=streak), \
             patch("triggers.location_change.dispatch"), \
             patch("triggers.location_change.get_address",
                   return_value={"city": "London", "suburb": None, "road": None,
                                 "region": None, "display_name": "London"}), \
             patch("triggers.location_change.LOCATION_CHANGE_RADIUS_M", CHANGE_M), \
             patch("triggers.location_change.LOCATION_DEPARTURE_CONFIRMATION_MINS", DEPART_MINS), \
             patch("triggers.location_change.LOCATION_STAY_DURATION_MINS", STAY_MINS), \
             patch("triggers.location_change.LOCATION_REVISIT_DURATION_MINS", REVISIT_MINS), \
             patch("triggers.location_change.LOCATION_MINIMUM_POINTS", MIN_POINTS):
            run()

    def test_no_streak_no_open_visits_noop(self, db):
        self._run(db, streak=None)
        assert db.execute("SELECT COUNT(*) FROM place_visits").fetchone()[0] == 0

    def test_open_visit_with_confirmed_departure_is_closed(self, db):
        lat, lon = LONDON
        place_id = _place(db, lat, lon)
        visit_id = _visit(db, place_id, _fixed(0))
        _set_cv(db, place_id, visit_id)
        _pt(db, lat, lon, _fixed(10))    # last in-radius
        _pt(db, *PARIS,   _fixed(20))   # subsequent out-of-radius
        self._run(db, streak=None)
        row = db.execute("SELECT departed_at FROM place_visits WHERE id = ?", (visit_id,)).fetchone()
        assert row["departed_at"] is not None

    def test_stationary_at_known_place_opens_visit(self, db):
        lat, lon = LONDON
        place_id = _place(db, lat, lon)
        streak = (lat, lon, _fixed(0), _fixed(REVISIT_MINS), 5)
        self._run(db, streak=streak)
        row = db.execute("SELECT id FROM place_visits WHERE place_id = ?", (place_id,)).fetchone()
        assert row is not None

    def test_stationary_at_new_location_creates_place(self, db):
        lat, lon = LONDON
        streak = (lat, lon, _fixed(0), _fixed(STAY_MINS), 5)
        self._run(db, streak=streak)
        assert db.execute("SELECT COUNT(*) FROM known_places").fetchone()[0] == 1
        assert db.execute("SELECT COUNT(*) FROM place_visits").fetchone()[0] == 1

    def test_departure_and_arrival_processed_in_same_run(self, db):
        """Departed from a known place; now at a new location — both recorded in one run."""
        # Set up a known place with an open visit that has been departed
        lat_a, lon_a = LONDON
        place_a = _place(db, lat_a, lon_a)
        visit_a = _visit(db, place_a, _fixed(0))
        _set_cv(db, place_a, visit_a)
        _pt(db, lat_a, lon_a, _fixed(10))   # last in-radius for A
        _pt(db, *PARIS,       _fixed(20))   # out-of-radius point

        # Streak is at PARIS (new location)
        lat_b, lon_b = PARIS
        streak = (lat_b, lon_b, _fixed(20), _fixed(20 + STAY_MINS), 5)
        self._run(db, streak=streak)

        # Visit A should be closed
        row_a = db.execute("SELECT departed_at FROM place_visits WHERE id = ?", (visit_a,)).fetchone()
        assert row_a["departed_at"] is not None

        # New place B should be created
        assert db.execute("SELECT COUNT(*) FROM known_places").fetchone()[0] == 2

    def test_check_departure_runs_before_detect_arrival(self, db):
        """Ensures departure is processed before arrival so current state is consistent."""
        lat, lon = LONDON
        place_id = _place(db, lat, lon)
        old_visit = _visit(db, place_id, _fixed(-120))
        _set_cv(db, place_id, old_visit)
        _pt(db, lat, lon, _fixed(-110))   # last in-radius for old visit
        _pt(db, *PARIS,   _fixed(-100))  # subsequent out-of-radius

        # Streak at LONDON — should open a new visit AFTER the old one is closed
        streak = (lat, lon, _fixed(0), _fixed(REVISIT_MINS), 5)
        self._run(db, streak=streak)

        visits = db.execute(
            "SELECT id, departed_at FROM place_visits ORDER BY id ASC"
        ).fetchall()
        assert visits[0]["departed_at"] is not None  # old visit closed
        assert visits[1]["departed_at"] is None       # new visit open
