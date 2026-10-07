# tests/test_backfill_place.py

import random
import sqlite3
from datetime import datetime, timedelta, timezone
from unittest.mock import patch, MagicMock

import pytest

from database.location.nearest_place import nearest_place_id, NEAREST_PLACE_VIEW_SQL
from scheduled_tasks import backfill_place

FMT = "%Y-%m-%dT%H:%M:%SZ"
BASE = datetime(2026, 10, 6, 12, 0, 0, tzinfo=timezone.utc)

SCHEMA = """
CREATE TABLE location_overland (
    id INTEGER PRIMARY KEY, timestamp TEXT NOT NULL, latitude REAL, longitude REAL,
    altitude REAL, motion TEXT, battery_level REAL, speed REAL, device_id TEXT,
    horizontal_accuracy REAL, place_id INTEGER);
CREATE INDEX idx_overland_timestamp ON location_overland(timestamp);
CREATE TABLE location_shortcuts (
    id INTEGER PRIMARY KEY, timestamp TEXT NOT NULL, latitude REAL, longitude REAL,
    altitude REAL, battery REAL, device TEXT, place_id INTEGER);
CREATE INDEX idx_lshortcuts_timestamp ON location_shortcuts(timestamp);
CREATE VIEW location_unified AS
    SELECT 'overland' AS source, o.id AS source_id, o.timestamp, o.latitude, o.longitude,
           o.altitude, o.motion AS activity, o.battery_level AS battery, o.speed,
           o.device_id AS device, o.horizontal_accuracy AS accuracy, o.place_id
    FROM location_overland o
    UNION ALL
    SELECT 'shortcuts', s.id, s.timestamp, s.latitude, s.longitude, s.altitude, NULL,
           CAST(s.battery AS REAL) / 100.0, NULL, s.device, NULL, s.place_id
    FROM location_shortcuts s
    WHERE NOT EXISTS (
        SELECT 1 FROM location_overland o
        WHERE o.timestamp BETWEEN datetime(s.timestamp, '-3 minutes')
                              AND datetime(s.timestamp, '+3 minutes'))
    ORDER BY timestamp ASC;
CREATE TABLE transactions (id TEXT, source TEXT, currency TEXT, timestamp TEXT, place_id INTEGER);
CREATE TABLE health_quantity (id INTEGER PRIMARY KEY, timestamp TEXT, place_id INTEGER);
CREATE TABLE health_heart_rate (id INTEGER PRIMARY KEY, timestamp TEXT, place_id INTEGER);
CREATE TABLE health_sleep (id INTEGER PRIMARY KEY, start_ts TEXT, duration_hr REAL, place_id INTEGER);
CREATE TABLE state_of_mind (id INTEGER PRIMARY KEY, start_ts TEXT, place_id INTEGER);
CREATE TABLE workouts (id INTEGER PRIMARY KEY, start_ts TEXT, start_place_id INTEGER);
CREATE TABLE trigger_log (id INTEGER PRIMARY KEY, fired_at TEXT, place_id INTEGER);
CREATE TABLE photo_metadata (id INTEGER PRIMARY KEY, taken_at TEXT, place_id INTEGER);
"""


def ts(offset_s: int, base: datetime = BASE) -> str:
    return (base + timedelta(seconds=offset_s)).strftime(FMT)


def _open(path, read_only=False):
    if read_only:
        c = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    else:
        c = sqlite3.connect(path, timeout=5)
    c.row_factory = sqlite3.Row
    return c


@pytest.fixture
def db_path(tmp_path):
    path = tmp_path / "travel.db"
    c = sqlite3.connect(path)
    c.execute("PRAGMA journal_mode=WAL")
    c.executescript(SCHEMA)
    c.commit()
    c.close()
    return path


@pytest.fixture
def conn(db_path):
    c = _open(db_path)
    yield c
    c.close()


def add_overland(conn, offset_s, place_id, base=BASE):
    conn.execute("INSERT INTO location_overland (timestamp, place_id) VALUES (?, ?)", (ts(offset_s, base), place_id))


def add_shortcut(conn, offset_s, place_id, base=BASE):
    conn.execute("INSERT INTO location_shortcuts (timestamp, place_id) VALUES (?, ?)", (ts(offset_s, base), place_id))


# --- nearest_place_id: semantics ---

def test_prefers_most_recent_fix_before_even_if_a_later_one_is_closer(conn):
    add_overland(conn, -300, 1)   # 5 min before
    add_overland(conn, +10, 2)    # 10 s after (closer, but "after")
    assert nearest_place_id(conn, ts(0), 900) == 1


def test_picks_nearest_among_fixes_before(conn):
    add_overland(conn, -600, 1)
    add_overland(conn, -60, 2)
    assert nearest_place_id(conn, ts(0), 900) == 2


def test_falls_back_to_earliest_fix_after_when_none_before(conn):
    add_overland(conn, +400, 3)
    add_overland(conn, +100, 4)
    assert nearest_place_id(conn, ts(0), 900) == 4


def test_exact_timestamp_counts_as_before(conn):
    add_overland(conn, 0, 5)
    add_overland(conn, +1, 6)
    assert nearest_place_id(conn, ts(0), 900) == 5


def test_window_is_inclusive_and_enforced(conn):
    add_overland(conn, -900, 7)
    assert nearest_place_id(conn, ts(0), 900) == 7        # exactly on the edge
    assert nearest_place_id(conn, ts(0), 899) is None     # just outside


def test_fixes_without_a_place_are_ignored(conn):
    add_overland(conn, -10, None)
    add_overland(conn, -500, 8)
    assert nearest_place_id(conn, ts(0), 900) == 8


def test_no_fix_in_window_returns_none(conn):
    add_overland(conn, -5000, 9)
    assert nearest_place_id(conn, ts(0), 900) is None


def test_shortcuts_fixes_are_used(conn):
    add_shortcut(conn, -120, 10)
    assert nearest_place_id(conn, ts(0), 900) == 10


def test_non_canonical_timestamp_uses_view_fallback(conn):
    add_overland(conn, -60, 11)
    assert nearest_place_id(conn, "2026-10-06 11:59:30", 900) == 11


def test_fast_path_does_not_touch_the_view(conn):
    add_overland(conn, -60, 12)
    seen = []
    conn.set_trace_callback(seen.append)
    nearest_place_id(conn, ts(0), 900)
    assert not any("location_unified" in s for s in seen)
    assert any("location_overland" in s for s in seen)


# --- fast path == legacy view path ---

def test_fast_and_legacy_paths_agree_on_random_data(conn):
    rng = random.Random(42)
    # Include shortcuts fixes just before UTC midnight, where the view's
    # datetime(...) de-dup comparison behaves unusually.
    days = [datetime(2026, 10, d, tzinfo=timezone.utc) for d in (5, 6, 7)]
    for _ in range(600):
        day = rng.choice(days)
        t = day + timedelta(seconds=rng.randrange(0, 86400))
        place = rng.choice([None, 1, 2, 3, 4, 5])
        table = rng.choice(["location_overland", "location_shortcuts"])
        conn.execute(f"INSERT INTO {table} (timestamp, place_id) VALUES (?, ?)", (t.strftime(FMT), place))
    for day in days:  # fixes around midnight on both tables
        for sec in (-170, -120, -30, 30, 150):
            add_shortcut(conn, sec, rng.choice([6, 7]), base=day)
            add_overland(conn, sec + 7, rng.choice([8, 9]), base=day)

    checked = hits = 0
    for _ in range(400):
        day = rng.choice(days)
        t = (day + timedelta(seconds=rng.randrange(-600, 86400 + 600))).strftime(FMT)
        window = rng.choice([900, 1800, 3600, 7200])
        legacy = conn.execute(NEAREST_PLACE_VIEW_SQL, {"ts": t, "window_s": window}).fetchone()
        legacy = legacy["place_id"] if legacy else None
        assert nearest_place_id(conn, t, window) == legacy, (t, window)
        checked += 1
        hits += legacy is not None
    assert hits > 100  # the data is dense enough that this is a real comparison


# --- the two-phase flow ---

def _seed_targets(db_path):
    c = _open(db_path)
    add_overland(c, -60, 21)
    c.execute("INSERT INTO health_quantity (timestamp) VALUES (?)", (ts(0),))
    c.execute("INSERT INTO health_quantity (timestamp) VALUES (?)", (ts(-5 * 86400),))   # no fix nearby
    c.execute("INSERT INTO health_heart_rate (timestamp) VALUES (?)", (ts(10),))
    c.execute("INSERT INTO transactions VALUES ('t1', 'revolut', 'GBP', ?, NULL)", (ts(0),))
    c.execute("INSERT INTO workouts (start_ts) VALUES (?)", (ts(0),))
    c.execute("INSERT INTO trigger_log (fired_at) VALUES (?)", (ts(0),))
    c.execute("INSERT INTO photo_metadata (taken_at) VALUES (?)", (ts(0),))
    c.execute("INSERT INTO state_of_mind (start_ts) VALUES (?)", (ts(0),))
    # sleep: starts 1 h before BASE, lasts 2 h -> midpoint == BASE
    c.execute("INSERT INTO health_sleep (start_ts, duration_hr) VALUES (?, 2.0)", (ts(-3600),))
    c.commit()
    c.close()


@pytest.fixture
def patched(db_path):
    def fake_get_conn(read_only=False):
        return _open(db_path, read_only=read_only)
    with patch("scheduled_tasks.backfill_place.get_conn", side_effect=fake_get_conn), \
         patch("scheduled_tasks.backfill_place.get_run_logger", return_value=MagicMock()):
        yield db_path


def test_backfill_fills_rows_and_reports_counts(patched):
    _seed_targets(patched)
    result = backfill_place.backfill_all_places.fn()

    assert result["health_quantity_found"] == 2
    assert result["health_quantity_backfilled"] == 1      # the 5-days-away row stays unmatched
    for key in ("transactions", "health_heart_rate", "health_sleep", "state_of_mind",
                "workouts", "trigger_log", "photo_metadata"):
        assert result[f"{key}_found"] == 1 and result[f"{key}_backfilled"] == 1, key

    c = _open(patched)
    assert [r[0] for r in c.execute("SELECT place_id FROM health_quantity ORDER BY id")] == [21, None]
    assert c.execute("SELECT place_id FROM health_sleep").fetchone()[0] == 21
    assert c.execute("SELECT start_place_id FROM workouts").fetchone()[0] == 21
    assert c.execute("SELECT place_id FROM transactions").fetchone()[0] == 21
    c.close()


def test_backfill_result_keys_match_original_flow(patched):
    result = backfill_place.backfill_all_places.fn()
    assert set(result) == {f"{k}_{s}" for k in (
        "transactions", "health_quantity", "health_sleep", "health_heart_rate",
        "state_of_mind", "workouts", "trigger_log", "photo_metadata") for s in ("found", "backfilled")}


def test_backfill_is_idempotent(patched):
    _seed_targets(patched)
    backfill_place.backfill_all_places.fn()
    second = backfill_place.backfill_all_places.fn()
    assert second["health_quantity_found"] == 1            # only the unmatched row is retried
    assert second["transactions_found"] == 0


def test_no_write_lock_is_held_while_looking_up_places(patched):
    """The point of the rewrite: lookups must not hold the DB write lock."""
    _seed_targets(patched)
    real = backfill_place.nearest_place_id
    lock_free = []

    def spying_lookup(conn, ts_, window_s):
        other = sqlite3.connect(patched, timeout=0.1)
        try:
            other.execute("BEGIN IMMEDIATE")   # raises "database is locked" if we held the lock
            other.execute("ROLLBACK")
            lock_free.append(True)
        finally:
            other.close()
        return real(conn, ts_, window_s)

    with patch("scheduled_tasks.backfill_place.nearest_place_id", side_effect=spying_lookup):
        backfill_place.backfill_all_places.fn()
    assert len(lock_free) >= 9


def test_rows_filled_by_someone_else_between_read_and_write_are_not_overwritten(patched):
    _seed_targets(patched)
    real_apply = backfill_place.apply_updates

    def apply_after_competitor_writes(resolved, *a, **kw):
        c = _open(patched)
        c.execute("UPDATE health_heart_rate SET place_id = 99")
        c.commit()
        c.close()
        return real_apply(resolved, *a, **kw)

    with patch("scheduled_tasks.backfill_place.apply_updates", side_effect=apply_after_competitor_writes):
        backfill_place.backfill_all_places.fn()
    c = _open(patched)
    assert c.execute("SELECT place_id FROM health_heart_rate").fetchone()[0] == 99
    c.close()


def test_updates_are_written_in_chunks(patched):
    c = _open(patched)
    add_overland(c, -60, 31)
    for i in range(7):
        c.execute("INSERT INTO photo_metadata (taken_at) VALUES (?)", (ts(i),))
    c.commit()
    c.close()

    commits = []
    real_get_conn = backfill_place.get_conn

    def counting_get_conn(read_only=False):
        conn = real_get_conn(read_only=read_only)
        if not read_only:
            commits.append(1)
        return conn

    with patch("scheduled_tasks.backfill_place._WRITE_CHUNK", 3), \
         patch("scheduled_tasks.backfill_place.get_conn", side_effect=counting_get_conn):
        result = backfill_place.backfill_all_places.fn()

    assert result["photo_metadata_backfilled"] == 7
    assert len(commits) == 3                                # 7 rows / chunk of 3
    c = _open(patched)
    assert c.execute("SELECT COUNT(*) FROM photo_metadata WHERE place_id = 31").fetchone()[0] == 7
    c.close()
