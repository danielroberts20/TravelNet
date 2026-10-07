# tests/test_location_noise_schedule_and_index.py

import sqlite3
from unittest.mock import patch

import pytest

from config.schedules import SCHEDULE_CONFIGS

# The tier-1 query from scheduled_tasks/flag_location_noise.py::flag_tier1_noise.
# Duplicated here on purpose: if the flow's SQL changes shape, update this too.
TIER1_SQL = """
    SELECT o.id, o.horizontal_accuracy
    FROM location_overland o
    WHERE o.horizontal_accuracy > ?
    AND NOT EXISTS (
        SELECT 1 FROM location_noise n WHERE n.overland_id = o.id
    )
"""


# --- schedule ---

def test_noise_flow_runs_once_a_day():
    cron, description = SCHEDULE_CONFIGS["identify-location-noise"]
    minute, hour, dom, month, dow = cron.split()
    assert minute.isdigit() and hour.isdigit()          # a single fixed time...
    assert (dom, month, dow) == ("*", "*", "*")         # ...every day, not hourly
    assert "Daily" in description


def test_noise_flow_does_not_share_a_slot_with_another_flow():
    cron = SCHEDULE_CONFIGS["identify-location-noise"][0]
    others = [c for name, (c, _) in SCHEDULE_CONFIGS.items() if name != "identify-location-noise" and c]
    assert cron not in others


# --- index ---

@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    # Both tables' init() use `with get_conn() as conn`, i.e. one connection object.
    with patch("database.location.overland.table.get_conn", return_value=c), \
         patch("database.location.noise.table.get_conn", return_value=c):
        from database.location.overland.table import table as overland
        from database.location.noise.table import table as noise
        overland.init()
        noise.init()
    yield c
    c.close()


def _plan(conn, threshold=100):
    return " / ".join(r[3] for r in conn.execute("EXPLAIN QUERY PLAN " + TIER1_SQL, (threshold,)))


def test_init_creates_the_accuracy_index(conn):
    names = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='location_overland'")}
    assert "idx_overland_accuracy" in names


def test_init_is_idempotent(conn):
    with patch("database.location.overland.table.get_conn", return_value=conn):
        from database.location.overland.table import table as overland
        overland.init()      # second call must not raise (CREATE INDEX IF NOT EXISTS)


def test_tier1_query_searches_by_the_index_instead_of_scanning(conn):
    for i in range(500):
        conn.execute(
            "INSERT INTO location_overland (device_id, timestamp, latitude, longitude, horizontal_accuracy) "
            "VALUES ('d', ?, 0, 0, ?)", (f"2026-10-06T00:{i // 60:02d}:{i % 60:02d}Z", 5 + (i % 200)))
    plan = _plan(conn)
    assert "idx_overland_accuracy" in plan
    assert "SCAN o" not in plan


def test_tier1_query_still_returns_the_same_rows(conn):
    for i, acc in enumerate([5, 99, 100, 101, 250, None]):
        conn.execute(
            "INSERT INTO location_overland (device_id, timestamp, latitude, longitude, horizontal_accuracy) "
            "VALUES ('d', ?, 0, 0, ?)", (f"2026-10-06T00:00:0{i}Z", acc))
    got = sorted(r["horizontal_accuracy"] for r in conn.execute(TIER1_SQL, (100,)))
    assert got == [101, 250]                              # strictly greater; NULL never matches


def test_index_is_also_used_for_a_different_threshold(conn):
    """The threshold is an editable setting, so the plan must not depend on its value."""
    for i in range(500):
        conn.execute(
            "INSERT INTO location_overland (device_id, timestamp, latitude, longitude, horizontal_accuracy) "
            "VALUES ('d', ?, 0, 0, ?)", (f"2026-10-06T00:{i // 60:02d}:{i % 60:02d}Z", 5 + (i % 200)))
    assert "idx_overland_accuracy" in _plan(conn, threshold=30)
