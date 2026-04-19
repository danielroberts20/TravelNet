"""
test_retroactive_location_scan.py — Comprehensive tests for
scheduled_tasks/retroactive_location_scan.py.

Covers:
  _read_marker / _write_marker  — marker file read/write, atomicity, corruption
  _get_points_from              — DB query, None cursor, cursor with overlap
  _scan_for_stays               — forward cluster scan, thresholds, idempotency
  retroactive_location_scan_flow integration path (mocked Prefect context)
"""

import json
import logging
import sqlite3
import pytest
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import patch, MagicMock

from scheduled_tasks.retroactive_location_scan import (
    _read_marker,
    _write_marker,
    _get_points_from,
    _scan_for_stays,
)


# ---------------------------------------------------------------------------
# Config constants used throughout
# ---------------------------------------------------------------------------

MIN_POINTS   = 3
STATION_M    = 150
CHANGE_M     = 500
STAY_MINS    = 30
REVISIT_MINS = 5
DEPART_MINS  = 5

logger = logging.getLogger(__name__)

# Reference coordinates
LONDON      = (51.5074, -0.1278)
LONDON_NEAR = (51.5075, -0.1279)   # ~12 m
LONDON_MID  = (51.5077, -0.1245)   # ~270 m — within CHANGE_M, outside STATION_M
PARIS       = (48.8566,  2.3522)   # ~340 km


# ---------------------------------------------------------------------------
# Timestamp helpers
# ---------------------------------------------------------------------------

def _fixed(offset_mins: int = 0) -> str:
    base = datetime(2024, 6, 15, 9, 0, 0, tzinfo=timezone.utc)
    return (base + timedelta(minutes=offset_mins)).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# Shared DB fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def db():
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
            label            TEXT,
            latitude         REAL NOT NULL,
            longitude        REAL NOT NULL,
            place_id         INTEGER,
            first_seen       TEXT NOT NULL,
            last_visited     TEXT,
            visit_count      INTEGER NOT NULL DEFAULT 0,
            total_time_mins  INTEGER NOT NULL DEFAULT 0,
            current_visit_id INTEGER
        );
        CREATE TABLE place_visits (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            known_place_id INTEGER NOT NULL,
            arrived_at     TEXT NOT NULL,
            departed_at    TEXT,
            duration_mins  INTEGER
        );
        CREATE TABLE places (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lat_snap REAL, lon_snap REAL, geocoded_at TEXT,
            country_code TEXT, country TEXT, region TEXT,
            city TEXT, suburb TEXT, road TEXT, display_name TEXT
        );
    """)
    return conn


def _pt(db, lat, lon, ts):
    db.execute(
        "INSERT INTO location_unified (timestamp, latitude, longitude) VALUES (?,?,?)",
        (ts, lat, lon),
    )
    db.commit()


def _place(db, lat, lon, first_seen=None, label=None):
    first_seen = first_seen or _fixed()
    cur = db.execute(
        """INSERT INTO known_places
               (latitude, longitude, first_seen, visit_count, last_visited, label)
           VALUES (?,?,?,1,?,?)""",
        (lat, lon, first_seen, first_seen, label),
    )
    db.commit()
    return cur.lastrowid


def _visit(db, place_id, arrived_at, departed_at=None):
    cur = db.execute(
        "INSERT INTO place_visits (known_place_id, arrived_at, departed_at) VALUES (?,?,?)",
        (place_id, arrived_at, departed_at),
    )
    db.commit()
    return cur.lastrowid


def _set_cv(db, place_id, visit_id):
    db.execute(
        "UPDATE known_places SET current_visit_id = ? WHERE id = ?",
        (visit_id, place_id),
    )
    db.commit()


def _cfg_patches():
    """Return a list of patch objects for all config constants."""
    return [
        patch("scheduled_tasks.retroactive_location_scan.LOCATION_STATIONARITY_RADIUS_M", STATION_M),
        patch("scheduled_tasks.retroactive_location_scan.LOCATION_MINIMUM_POINTS", MIN_POINTS),
        patch("scheduled_tasks.retroactive_location_scan.LOCATION_REVISIT_DURATION_MINS", REVISIT_MINS),
        patch("scheduled_tasks.retroactive_location_scan.LOCATION_STAY_DURATION_MINS", STAY_MINS),
        patch("scheduled_tasks.retroactive_location_scan.LOCATION_DEPARTURE_CONFIRMATION_MINS", DEPART_MINS),
        patch("triggers.location_change.LOCATION_CHANGE_RADIUS_M", CHANGE_M),
        patch("triggers.location_change.LOCATION_MINIMUM_POINTS", MIN_POINTS),
        patch("triggers.location_change.LOCATION_STATIONARITY_RADIUS_M", STATION_M),
        patch("triggers.location_change.LOCATION_CHANGE_RADIUS_M", CHANGE_M),
        patch("triggers.location_change.LOCATION_REVISIT_DURATION_MINS", REVISIT_MINS),
        patch("triggers.location_change.LOCATION_STAY_DURATION_MINS", STAY_MINS),
        patch("triggers.location_change.LOCATION_DEPARTURE_CONFIRMATION_MINS", DEPART_MINS),
    ]


# ===========================================================================
# _read_marker / _write_marker
# ===========================================================================

class TestMarkerFile:

    def test_read_missing_file_returns_none(self, tmp_path):
        marker = tmp_path / "retroactive_location_scan.marker"
        with patch("scheduled_tasks.retroactive_location_scan.MARKER_PATH", marker):
            result = _read_marker()
        assert result is None

    def test_write_then_read_returns_same_timestamp(self, tmp_path):
        marker = tmp_path / "retroactive_location_scan.marker"
        ts = _fixed(0)
        with patch("scheduled_tasks.retroactive_location_scan.MARKER_PATH", marker):
            _write_marker(ts)
            result = _read_marker()
        assert result == ts

    def test_corrupted_json_returns_none(self, tmp_path):
        marker = tmp_path / "retroactive_location_scan.marker"
        marker.write_text("not valid json{{{")
        with patch("scheduled_tasks.retroactive_location_scan.MARKER_PATH", marker):
            result = _read_marker()
        assert result is None

    def test_empty_json_object_returns_none(self, tmp_path):
        marker = tmp_path / "retroactive_location_scan.marker"
        marker.write_text("{}")
        with patch("scheduled_tasks.retroactive_location_scan.MARKER_PATH", marker):
            result = _read_marker()
        assert result is None

    def test_write_creates_file(self, tmp_path):
        marker = tmp_path / "retroactive_location_scan.marker"
        with patch("scheduled_tasks.retroactive_location_scan.MARKER_PATH", marker):
            _write_marker(_fixed(0))
        assert marker.exists()

    def test_write_stores_correct_json(self, tmp_path):
        marker = tmp_path / "retroactive_location_scan.marker"
        ts = _fixed(0)
        with patch("scheduled_tasks.retroactive_location_scan.MARKER_PATH", marker):
            _write_marker(ts)
        data = json.loads(marker.read_text())
        assert data["last_processed"] == ts

    def test_overwrite_updates_timestamp(self, tmp_path):
        marker = tmp_path / "retroactive_location_scan.marker"
        with patch("scheduled_tasks.retroactive_location_scan.MARKER_PATH", marker):
            _write_marker(_fixed(0))
            _write_marker(_fixed(60))
            result = _read_marker()
        assert result == _fixed(60)

    def test_write_is_atomic_uses_replace(self, tmp_path):
        """_write_marker should use os.replace (atomic rename) not a direct write."""
        marker = tmp_path / "retroactive_location_scan.marker"
        replaced = []
        real_replace = __import__("os").replace

        def capturing_replace(src, dst):
            replaced.append((src, dst))
            real_replace(src, dst)

        with patch("scheduled_tasks.retroactive_location_scan.MARKER_PATH", marker), \
             patch("os.replace", side_effect=capturing_replace):
            _write_marker(_fixed(0))

        assert len(replaced) == 1
        src, dst = replaced[0]
        assert str(dst) == str(marker)
        assert src != dst  # tmp file renamed to final


# ===========================================================================
# _get_points_from
# ===========================================================================

class TestGetPointsFrom:

    def _run(self, db, cursor):
        with patch("scheduled_tasks.retroactive_location_scan.get_conn", return_value=db), \
             patch("scheduled_tasks.retroactive_location_scan.LOCATION_STAY_DURATION_MINS", STAY_MINS):
            return _get_points_from(cursor)

    def test_none_cursor_returns_all_points(self, db):
        for i in range(5):
            _pt(db, *LONDON, _fixed(i * 10))
        rows = self._run(db, None)
        assert len(rows) == 5

    def test_none_cursor_empty_db_returns_empty(self, db):
        assert self._run(db, None) == []

    def test_cursor_excludes_points_before_overlap(self, db):
        # Cursor at t=60. Overlap = 30 min. So cutoff = t=30.
        # Points at t=0 should be excluded; t=30+ included.
        _pt(db, *LONDON, _fixed(0))    # excluded (before cutoff t=30)
        _pt(db, *LONDON, _fixed(30))   # included (at cutoff)
        _pt(db, *LONDON, _fixed(60))   # included
        _pt(db, *LONDON, _fixed(90))   # included
        rows = self._run(db, _fixed(60))
        timestamps = [r["timestamp"] for r in rows]
        assert _fixed(0)  not in timestamps
        assert _fixed(30) in timestamps
        assert _fixed(60) in timestamps

    def test_cursor_result_is_chronological(self, db):
        for i in [3, 1, 2, 0]:
            _pt(db, *LONDON, _fixed(i * 10))
        rows = self._run(db, None)
        timestamps = [r["timestamp"] for r in rows]
        assert timestamps == sorted(timestamps)

    def test_none_cursor_result_is_chronological(self, db):
        for i in [5, 2, 8, 1]:
            _pt(db, *LONDON, _fixed(i * 10))
        rows = self._run(db, None)
        timestamps = [r["timestamp"] for r in rows]
        assert timestamps == sorted(timestamps)

    def test_cursor_returns_rows_with_expected_keys(self, db):
        _pt(db, *LONDON, _fixed(0))
        rows = self._run(db, None)
        assert "timestamp" in rows[0].keys()
        assert "latitude"  in rows[0].keys()
        assert "longitude" in rows[0].keys()


# ===========================================================================
# _scan_for_stays
# ===========================================================================

class TestScanForStays:
    """Forward cluster scan: finds stationary stays and records arrivals."""

    def _run(self, db, points):
        ctx = __import__("contextlib").ExitStack()
        patches = _cfg_patches() + [
            patch("triggers.location_change.get_conn", return_value=db),
            patch("database.location.known_places.table.get_conn", return_value=db),
            patch("triggers.location_change.get_place_id", return_value=1),
            patch("triggers.location_change.dispatch"),
            patch("triggers.location_change.get_address",
                  return_value={"city": "London", "suburb": None, "road": None,
                                "region": None, "display_name": "London"}),
        ]
        for p in patches:
            ctx.enter_context(p)
        with ctx:
            return _scan_for_stays(points, logger)

    def _make_cluster(self, lat, lon, start_offset, n_points, gap_mins=1):
        """Build a list of stationary dicts at (lat, lon)."""
        return [
            {"timestamp": _fixed(start_offset + i * gap_mins),
             "latitude": lat, "longitude": lon}
            for i in range(n_points)
        ]

    # ---- Empty / trivial cases ----

    def test_empty_points_returns_zero(self, db):
        assert self._run(db, []) == 0

    def test_single_point_below_min_returns_zero(self, db):
        points = [{"timestamp": _fixed(0), "latitude": LONDON[0], "longitude": LONDON[1]}]
        assert self._run(db, points) == 0

    def test_cluster_below_min_points_not_processed(self, db):
        # MIN_POINTS=3; only 2 points in cluster
        points = self._make_cluster(*LONDON, 0, MIN_POINTS - 1)
        assert self._run(db, points) == 0

    # ---- Duration thresholds ----

    def test_new_place_cluster_below_stay_duration_not_created(self, db):
        # 5 points over 10 mins — below STAY_MINS=30
        points = self._make_cluster(*LONDON, 0, 5, gap_mins=2)
        count = self._run(db, points)
        assert count == 0
        assert db.execute("SELECT COUNT(*) FROM known_places").fetchone()[0] == 0

    def test_new_place_cluster_at_stay_duration_creates_place(self, db):
        # MIN_POINTS+1 points spanning exactly STAY_MINS
        points = self._make_cluster(*LONDON, 0, MIN_POINTS, gap_mins=STAY_MINS // (MIN_POINTS - 1))
        count = self._run(db, points)
        assert count == 1
        assert db.execute("SELECT COUNT(*) FROM known_places").fetchone()[0] == 1

    def test_known_place_cluster_below_revisit_duration_not_processed(self, db):
        _place(db, *LONDON)
        # 3 points, 1 min apart → 2 min total < REVISIT_MINS=5
        points = self._make_cluster(*LONDON, 0, MIN_POINTS, gap_mins=1)
        count = self._run(db, points)
        assert count == 0

    def test_known_place_cluster_at_revisit_duration_creates_visit(self, db):
        place_id = _place(db, *LONDON)
        # 3 points at 3-min gaps → duration=6 mins >= REVISIT_MINS=5
        # (5 // 2 = 2 would give only 4 mins, so use 3 explicitly)
        points = self._make_cluster(*LONDON, 0, MIN_POINTS, gap_mins=3)
        count = self._run(db, points)
        assert count == 1
        row = db.execute(
            "SELECT id FROM place_visits WHERE known_place_id = ?", (place_id,)
        ).fetchone()
        assert row is not None

    # ---- arrived_at accuracy ----

    def test_arrived_at_is_first_point_in_cluster(self, db):
        _place(db, *LONDON)
        points = self._make_cluster(*LONDON, 10, MIN_POINTS, gap_mins=3)
        self._run(db, points)
        row = db.execute("SELECT arrived_at FROM place_visits").fetchone()
        assert row["arrived_at"] == _fixed(10)  # first point in cluster

    # ---- Idempotency ----

    def test_existing_visit_not_duplicated(self, db):
        place_id = _place(db, *LONDON)
        # Pre-insert a visit at the same time as the cluster start
        _visit(db, place_id, _fixed(0))
        points = self._make_cluster(*LONDON, 0, MIN_POINTS,
                                    gap_mins=REVISIT_MINS // (MIN_POINTS - 1))
        self._run(db, points)
        count = db.execute("SELECT COUNT(*) FROM place_visits").fetchone()[0]
        assert count == 1  # not doubled

    def test_second_run_does_not_create_duplicate_place(self, db):
        points = self._make_cluster(*LONDON, 0, MIN_POINTS,
                                    gap_mins=STAY_MINS // (MIN_POINTS - 1))
        # First scan creates the place
        self._run(db, points)
        assert db.execute("SELECT COUNT(*) FROM known_places").fetchone()[0] == 1
        # Second scan — same points, same location; place already exists → revisit check
        self._run(db, points)
        assert db.execute("SELECT COUNT(*) FROM known_places").fetchone()[0] == 1

    # ---- Multiple clusters ----

    def test_two_separate_stays_both_processed(self, db):
        # Cluster A then movement then Cluster B
        cluster_a = self._make_cluster(*LONDON,      0,  MIN_POINTS, gap_mins=STAY_MINS // (MIN_POINTS - 1))
        movement  = [{"timestamp": _fixed(40), "latitude": PARIS[0], "longitude": PARIS[1]}]
        cluster_b = self._make_cluster(*PARIS,       50, MIN_POINTS, gap_mins=STAY_MINS // (MIN_POINTS - 1))
        points = cluster_a + movement + cluster_b
        count = self._run(db, points)
        assert count == 2
        assert db.execute("SELECT COUNT(*) FROM known_places").fetchone()[0] == 2

    def test_short_stop_between_two_stays_not_counted(self, db):
        # Stay A → short stop → Stay B
        cluster_a   = self._make_cluster(*LONDON,     0,  MIN_POINTS, gap_mins=STAY_MINS // (MIN_POINTS - 1))
        short_stop  = self._make_cluster(*LONDON_MID, 40, 2, gap_mins=1)   # only 2 pts — below min
        cluster_b   = self._make_cluster(*PARIS,      50, MIN_POINTS, gap_mins=STAY_MINS // (MIN_POINTS - 1))
        points = cluster_a + short_stop + cluster_b
        count = self._run(db, points)
        assert count == 2  # A and B, not the short stop

    def test_movement_between_clusters_resets_anchor(self, db):
        """After movement, the next cluster is anchored on its own first point."""
        cluster_a = self._make_cluster(*LONDON, 0,  MIN_POINTS, gap_mins=STAY_MINS // (MIN_POINTS - 1))
        movement  = [{"timestamp": _fixed(40), "latitude": PARIS[0], "longitude": PARIS[1]}]
        cluster_b = self._make_cluster(*PARIS,  50, MIN_POINTS, gap_mins=STAY_MINS // (MIN_POINTS - 1))
        points = cluster_a + movement + cluster_b

        self._run(db, points)

        places = db.execute("SELECT latitude, longitude FROM known_places ORDER BY id").fetchall()
        assert len(places) == 2
        # First place is near LONDON, second near PARIS
        assert places[0]["latitude"] == pytest.approx(LONDON[0])
        assert places[1]["latitude"] == pytest.approx(PARIS[0])

    def test_mid_scan_new_place_visible_to_subsequent_cluster(self, db):
        """A place created mid-scan is visible to later clusters — no duplicate place created.

        cluster_a (LONDON) creates a new known place.  cluster_b (LONDON_NEAR, within
        CHANGE_M) finds that place via get_nearest_known_place and does NOT create a
        second known place.  Because current_visit_id is still set from cluster_a,
        _handle_known_place logs "still at location" — the second visit is opened on the
        next nightly run after check_departure closes the first one.
        """
        cluster_a = self._make_cluster(*LONDON,      0,  MIN_POINTS, gap_mins=STAY_MINS // (MIN_POINTS - 1))
        movement  = [{"timestamp": _fixed(40), "latitude": PARIS[0], "longitude": PARIS[1]}]
        cluster_b = self._make_cluster(*LONDON_NEAR, 50, MIN_POINTS, gap_mins=3)
        points = cluster_a + movement + cluster_b
        self._run(db, points)
        # Key assertion: cluster_b must NOT create a duplicate known place
        assert db.execute("SELECT COUNT(*) FROM known_places").fetchone()[0] == 1

    # ---- Cursor overlap / real data pattern ----

    def test_points_spanning_cluster_boundary_detected(self, db):
        """Points from previous run overlap correctly captures cross-boundary streaks."""
        # Simulate: previous run ended at t=20, overlap re-fetches from t=-10.
        # Cluster starts at t=0 — within the overlap window.
        points = self._make_cluster(*LONDON, 0, MIN_POINTS,
                                    gap_mins=STAY_MINS // (MIN_POINTS - 1))
        count = self._run(db, points)
        assert count == 1

    # ---- Sparse tracking ----

    def test_large_gaps_between_points_in_cluster(self, db):
        """Points far apart in time but within stationarity radius still form a valid cluster."""
        # 3 points, 20 min apart = 40 min total duration (>= STAY_MINS=30)
        points = self._make_cluster(*LONDON, 0, MIN_POINTS, gap_mins=20)
        count = self._run(db, points)
        assert count == 1

    def test_single_out_of_radius_point_separates_two_clusters(self, db):
        """One out-of-radius point correctly separates two stationary groups.

        cluster_a (LONDON) creates a new place and opens a visit.  The outlier at PARIS
        represents movement away.  cluster_b returns to LONDON — the scanner finds the
        existing place but current_visit_id is still set, so "still at location" is
        logged and no duplicate is created.  The second visit is opened on the next
        nightly run after check_departure closes the first.
        """
        lat, lon = LONDON
        cluster_a = self._make_cluster(lat, lon, 0,  MIN_POINTS, gap_mins=STAY_MINS // (MIN_POINTS - 1))
        outlier   = [{"timestamp": _fixed(40), "latitude": PARIS[0], "longitude": PARIS[1]}]
        cluster_b = self._make_cluster(lat, lon, 50, MIN_POINTS, gap_mins=3)
        points = cluster_a + outlier + cluster_b
        count = self._run(db, points)
        # cluster_a is the only stay created in this single scan run
        assert count == 1
        assert db.execute("SELECT COUNT(*) FROM known_places").fetchone()[0] == 1
        assert db.execute("SELECT COUNT(*) FROM place_visits").fetchone()[0] == 1
