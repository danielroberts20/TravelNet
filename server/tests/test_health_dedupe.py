"""Tests for database/health/dedupe.py: NULL-safe uniqueness on the health tables."""

import sqlite3

import pytest

from database.health.dedupe import (
    HEALTH_HEART_RATE, HEALTH_QUANTITY, HEALTH_SLEEP,
    duplicate_count, ensure_null_safe_unique,
)

HQ = """CREATE TABLE health_quantity (
    id INTEGER PRIMARY KEY, timestamp TEXT NOT NULL, metric TEXT NOT NULL, value REAL NOT NULL,
    unit TEXT NOT NULL, source TEXT, place_id INTEGER, UNIQUE(timestamp, metric, source))"""


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    c.execute(HQ)
    return c


def add(c, ts, metric, value, source=None):
    c.execute("INSERT OR IGNORE INTO health_quantity(timestamp, metric, value, unit, source) VALUES (?,?,?,?,?)",
              (ts, metric, value, "u", source))


def rows(c):
    return c.execute("SELECT id, timestamp, metric, value, source FROM health_quantity ORDER BY id").fetchall()


def test_old_constraint_lets_null_source_duplicate_in(conn):
    add(conn, "t1", "Step Count", 5)
    add(conn, "t1", "Step Count", 5)
    assert len(rows(conn)) == 2          # the bug this module fixes


def test_dedupe_keeps_earliest_and_counts(conn):
    add(conn, "t1", "Step Count", 5)
    add(conn, "t1", "Step Count", 9)     # differing value: first write wins
    add(conn, "t1", "Step Count", 5)
    add(conn, "t2", "Step Count", 1)
    assert duplicate_count(conn, HEALTH_QUANTITY) == 2
    assert ensure_null_safe_unique(conn, HEALTH_QUANTITY) == 2
    assert [(r[0], r[3]) for r in rows(conn)] == [(1, 5), (4, 1)]


def test_after_fix_null_source_insert_is_ignored(conn):
    add(conn, "t1", "Step Count", 5)
    ensure_null_safe_unique(conn, HEALTH_QUANTITY)
    add(conn, "t1", "Step Count", 99)
    assert len(rows(conn)) == 1 and rows(conn)[0][3] == 5


def test_distinct_keys_untouched(conn):
    add(conn, "t1", "Step Count", 5)
    add(conn, "t1", "Active Energy", 5)          # other metric
    add(conn, "t2", "Step Count", 5)             # other time
    add(conn, "t1", "Step Count", 5, "Watch")    # other source
    add(conn, "t1", "Step Count", 5, "Phone")
    assert ensure_null_safe_unique(conn, HEALTH_QUANTITY) == 0
    assert len(rows(conn)) == 5


def test_null_and_empty_source_are_the_same_key(conn):
    add(conn, "t1", "Step Count", 5, None)
    add(conn, "t1", "Step Count", 5, "")
    ensure_null_safe_unique(conn, HEALTH_QUANTITY)
    assert len(rows(conn)) == 1


def test_idempotent_second_call_is_a_noop(conn):
    add(conn, "t1", "Step Count", 5)
    add(conn, "t1", "Step Count", 5)
    assert ensure_null_safe_unique(conn, HEALTH_QUANTITY) == 1
    assert ensure_null_safe_unique(conn, HEALTH_QUANTITY) == 0


def test_works_on_empty_table(conn):
    assert ensure_null_safe_unique(conn, HEALTH_QUANTITY) == 0


def test_sourced_duplicates_never_existed_and_stay_deduped(conn):
    add(conn, "t1", "Step Count", 5, "Watch")
    add(conn, "t1", "Step Count", 5, "Watch")
    ensure_null_safe_unique(conn, HEALTH_QUANTITY)
    assert len(rows(conn)) == 1


def test_heart_rate_and_sleep_keys():
    c = sqlite3.connect(":memory:")
    c.execute("CREATE TABLE health_heart_rate (id INTEGER PRIMARY KEY, timestamp TEXT NOT NULL, avg_bpm REAL, source TEXT, UNIQUE(timestamp, source))")
    c.execute("CREATE TABLE health_sleep (id INTEGER PRIMARY KEY, start_ts TEXT NOT NULL, stage TEXT NOT NULL, source TEXT, UNIQUE(start_ts, stage, source))")
    for _ in range(3):
        c.execute("INSERT OR IGNORE INTO health_heart_rate(timestamp, avg_bpm, source) VALUES ('t', 60, NULL)")
        c.execute("INSERT OR IGNORE INTO health_sleep(start_ts, stage, source) VALUES ('t', 'Core', NULL)")
    c.execute("INSERT INTO health_sleep(start_ts, stage, source) VALUES ('t', 'REM', NULL)")
    assert ensure_null_safe_unique(c, HEALTH_HEART_RATE) == 2
    assert ensure_null_safe_unique(c, HEALTH_SLEEP) == 2
    assert c.execute("SELECT COUNT(*) FROM health_sleep").fetchone()[0] == 2     # Core + REM
    c.execute("INSERT OR IGNORE INTO health_heart_rate(timestamp, avg_bpm, source) VALUES ('t', 61, NULL)")
    assert c.execute("SELECT COUNT(*) FROM health_heart_rate").fetchone()[0] == 1
