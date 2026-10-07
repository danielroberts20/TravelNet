# tests/test_power_poll.py

import sqlite3
import threading
from unittest.mock import patch, MagicMock

import pytest

from database.power.table import PowerDailyTable, merge_reading
from scheduled_tasks import poll_shelly


@pytest.fixture(autouse=True)
def _patch_run_logger():
    with patch("scheduled_tasks.poll_shelly.get_run_logger"):
        yield


@pytest.fixture
def conn(tmp_path):
    """File-backed DB (not :memory:) so a second connection can contend for the lock."""
    path = tmp_path / "power.db"
    c = sqlite3.connect(path, timeout=5)
    c.execute("PRAGMA journal_mode=WAL")
    with patch("database.power.table.get_conn", return_value=c):
        PowerDailyTable().init()
    yield c
    c.close()


# --- merge_reading (pure) ---

def test_merge_first_reading_of_day():
    rec = merge_reading(None, "2026-10-07", 7.234, 100.0)
    assert (rec.min_w, rec.max_w, rec.avg_w, rec.readings) == (7.23, 7.23, 7.23, 1)
    assert rec.start_wh == 100.0 and rec.end_wh == 100.0


def test_merge_updates_min_max_avg_and_keeps_start():
    existing = {"min_w": 6.0, "max_w": 8.0, "avg_w": 7.0, "readings": 2, "start_wh": 100.0}
    rec = merge_reading(existing, "2026-10-07", 10.0, 101.5)
    assert rec.min_w == 6.0
    assert rec.max_w == 10.0
    assert rec.avg_w == 8.0          # (7*2 + 10) / 3
    assert rec.readings == 3
    assert rec.start_wh == 100.0     # unchanged
    assert rec.end_wh == 101.5       # latest
    assert rec.total_wh == 1.5


# --- upsert_reading (atomic read-modify-write) ---

def test_upsert_reading_creates_then_updates_row(conn):
    table = PowerDailyTable()
    table.upsert_reading("2026-10-07", 5.0, 10.0, conn=conn)
    rec = table.upsert_reading("2026-10-07", 7.0, 10.4, conn=conn)

    rows = conn.execute("SELECT date, min_w, max_w, avg_w, readings, start_wh, end_wh FROM power_daily").fetchall()
    assert rows == [("2026-10-07", 5.0, 7.0, 6.0, 2, 10.0, 10.4)]
    assert rec.readings == 2


def test_upsert_reading_separate_days_get_separate_rows(conn):
    table = PowerDailyTable()
    table.upsert_reading("2026-10-07", 5.0, 10.0, conn=conn)
    table.upsert_reading("2026-10-08", 9.0, 10.2, conn=conn)
    assert conn.execute("SELECT COUNT(*) FROM power_daily").fetchone()[0] == 2


def test_upsert_reading_rolls_back_and_restores_connection_state_on_error(conn):
    table = PowerDailyTable()
    table.upsert_reading("2026-10-07", 5.0, 10.0, conn=conn)
    before = conn.isolation_level

    with patch("database.power.table.merge_reading", side_effect=RuntimeError("boom")):
        with pytest.raises(RuntimeError):
            table.upsert_reading("2026-10-07", 7.0, 10.4, conn=conn)

    assert not conn.in_transaction
    assert conn.isolation_level == before
    assert conn.execute("SELECT readings FROM power_daily").fetchone()[0] == 1


def test_upsert_reading_takes_write_lock_up_front(conn):
    """BEGIN IMMEDIATE means a second writer is blocked at the start, not
    midway between its read and its write (the lost-update window)."""
    table = PowerDailyTable()
    conn.execute("PRAGMA busy_timeout=200")
    db_file = conn.execute("PRAGMA database_list").fetchone()[2]
    other = sqlite3.connect(db_file, timeout=0.2)
    other.execute("BEGIN IMMEDIATE")  # hold the write lock
    try:
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            table.upsert_reading("2026-10-07", 5.0, 10.0, conn=conn)
    finally:
        other.execute("ROLLBACK")
        other.close()
    # lock released: now it works and nothing was half-written
    assert conn.execute("SELECT COUNT(*) FROM power_daily").fetchone()[0] == 0
    table.upsert_reading("2026-10-07", 5.0, 10.0, conn=conn)
    assert conn.execute("SELECT COUNT(*) FROM power_daily").fetchone()[0] == 1


def test_concurrent_readings_are_not_lost(tmp_path):
    """Many threads each record one reading; the final count must equal the number of calls."""
    path = tmp_path / "concurrent.db"
    setup = sqlite3.connect(path, timeout=10)
    setup.execute("PRAGMA journal_mode=WAL")
    with patch("database.power.table.get_conn", return_value=setup):
        PowerDailyTable().init()
    setup.close()

    errors = []

    def worker():
        c = sqlite3.connect(path, timeout=30)
        try:
            PowerDailyTable().upsert_reading("2026-10-07", 5.0, 10.0, conn=c)
        except Exception as e:  # pragma: no cover - only on failure
            errors.append(e)
        finally:
            c.close()

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    check = sqlite3.connect(path)
    assert errors == []
    assert check.execute("SELECT readings FROM power_daily").fetchone()[0] == 8
    check.close()


# --- fetch_shelly_reading / flow ---

def test_fetch_shelly_reading_parses_response():
    resp = MagicMock()
    resp.json.return_value = {"apower": 7.5, "aenergy": {"total": 1234.5}}
    with patch("scheduled_tasks.poll_shelly.requests.post", return_value=resp), \
         patch("scheduled_tasks.poll_shelly.settings") as s:
        s.shelly_ip = "10.0.0.5"
        assert poll_shelly.fetch_shelly_reading() == {"apower": 7.5, "aenergy_total": 1234.5}


def test_flow_skips_upsert_when_shelly_unreachable():
    with patch("scheduled_tasks.poll_shelly.fetch_shelly_reading", side_effect=OSError("down")), \
         patch("scheduled_tasks.poll_shelly.power_table") as table:
        poll_shelly.poll_shelly_flow.fn()
    table.upsert_reading.assert_not_called()


def test_flow_upserts_reading_for_today_utc():
    rec = MagicMock(min_w=1, max_w=2, avg_w=1.5, total_wh=0.1, readings=3)
    with patch("scheduled_tasks.poll_shelly.fetch_shelly_reading",
               return_value={"apower": 7.5, "aenergy_total": 12.0}), \
         patch("scheduled_tasks.poll_shelly.power_table") as table:
        table.upsert_reading.return_value = rec
        poll_shelly.poll_shelly_flow.fn()
    date, watts, energy = table.upsert_reading.call_args.args
    assert len(date) == 10 and date[4] == "-"
    assert (watts, energy) == (7.5, 12.0)
