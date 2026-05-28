"""
tests/weather/test_weather_fetch_log.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Unit tests for database/weather/fetch_log.py (WeatherFetchLog) and the
_contiguous_ranges() helper in get_weather.py.

Uses an in-memory SQLite connection injected via monkeypatched get_conn so no
real database file is touched.
"""
import sqlite3
from datetime import date, timedelta

import pytest

from database.weather.fetch_log import WeatherFetchLog
from scheduled_tasks.get_weather import _contiguous_ranges


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


@pytest.fixture
def fetch_log(db, monkeypatch):
    monkeypatch.setattr("database.weather.fetch_log.get_conn", lambda *a, **kw: db)
    wfl = WeatherFetchLog()
    wfl.init()
    return wfl


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

D1 = date(2025, 1, 1)
D2 = date(2025, 1, 2)
D3 = date(2025, 1, 3)

START = D1
END   = D3  # 3-day window for most window tests


def _record(wfl, lat=1.0, lon=2.0, d=D1, **overrides):
    defaults = dict(
        hourly_ok=True, uv_ok=True, daily_ok=True,
        hourly_rows=24, daily_rows=1,
        error_hourly=None, error_uv=None, error_daily=None,
    )
    defaults.update(overrides)
    wfl.record(lat=lat, lon=lon, date=d, **defaults)


def _record_window(wfl, lat=1.0, lon=2.0, start=START, end=END, **overrides):
    """Insert one complete row per date in [start, end]."""
    d = start
    while d <= end:
        _record(wfl, lat=lat, lon=lon, d=d, **overrides)
        d += timedelta(days=1)


# ---------------------------------------------------------------------------
# init()
# ---------------------------------------------------------------------------

class TestInit:

    def test_creates_table(self, fetch_log, db):
        tables = {
            row[0] for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert "weather_fetch_log" in tables

    def test_creates_indexes(self, fetch_log, db):
        indexes = {
            row[0] for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='index'"
            ).fetchall()
        }
        assert "idx_weather_fetch_log_date" in indexes
        assert "idx_weather_fetch_log_lat_lon" in indexes

    def test_init_is_idempotent(self, fetch_log):
        fetch_log.init()  # second call must not raise

# ---------------------------------------------------------------------------
# record()
# ---------------------------------------------------------------------------

class TestRecord:

    def test_inserts_new_row(self, fetch_log, db):
        _record(fetch_log, lat=1.0, lon=2.0)
        count = db.execute("SELECT COUNT(*) FROM weather_fetch_log").fetchone()[0]
        assert count == 1

    def test_row_fields_stored_correctly(self, fetch_log, db):
        _record(fetch_log, lat=1.0, lon=2.0, d=D1, hourly_rows=24, daily_rows=1)
        row = db.execute("SELECT * FROM weather_fetch_log").fetchone()
        assert row["latitude"]  == pytest.approx(1.0)
        assert row["longitude"] == pytest.approx(2.0)
        assert row["date"] == D1.isoformat()
        assert row["hourly_ok"] == 1
        assert row["uv_ok"]     == 1
        assert row["daily_ok"]  == 1
        assert row["hourly_rows"] == 24
        assert row["daily_rows"]  == 1
        assert row["fetched_at"] is not None

    def test_second_call_updates_not_duplicates(self, fetch_log, db):
        _record(fetch_log, lat=1.0, lon=2.0, d=D1, hourly_ok=False, uv_ok=False, daily_ok=False)
        _record(fetch_log, lat=1.0, lon=2.0, d=D1, hourly_ok=True,  uv_ok=True,  daily_ok=True)

        count = db.execute("SELECT COUNT(*) FROM weather_fetch_log").fetchone()[0]
        assert count == 1

        row = db.execute("SELECT * FROM weather_fetch_log").fetchone()
        assert row["hourly_ok"] == 1
        assert row["uv_ok"]     == 1
        assert row["daily_ok"]  == 1

    def test_different_dates_stored_as_separate_rows(self, fetch_log, db):
        _record(fetch_log, lat=1.0, lon=2.0, d=D1)
        _record(fetch_log, lat=1.0, lon=2.0, d=D2)
        count = db.execute("SELECT COUNT(*) FROM weather_fetch_log").fetchone()[0]
        assert count == 2

    def test_different_cells_stored_separately(self, fetch_log, db):
        _record(fetch_log, lat=1.0, lon=2.0, d=D1)
        _record(fetch_log, lat=3.0, lon=4.0, d=D1)
        count = db.execute("SELECT COUNT(*) FROM weather_fetch_log").fetchone()[0]
        assert count == 2

    def test_stores_error_strings(self, fetch_log, db):
        _record(fetch_log, lat=5.0, lon=6.0, hourly_ok=False, error_hourly="timeout")
        row = db.execute("SELECT * FROM weather_fetch_log").fetchone()
        assert row["error_hourly"] == "timeout"


# ---------------------------------------------------------------------------
# get_complete_cells()
# ---------------------------------------------------------------------------

class TestGetCompleteCells:

    def test_complete_when_all_dates_covered(self, fetch_log):
        _record_window(fetch_log, lat=1.0, lon=2.0)  # inserts D1, D2, D3 all ok
        result = fetch_log.get_complete_cells(START, END)
        assert (1.0, 2.0) in result

    def test_incomplete_when_one_date_missing(self, fetch_log):
        # Only D1 and D2 inserted — D3 missing
        _record(fetch_log, lat=1.0, lon=2.0, d=D1)
        _record(fetch_log, lat=1.0, lon=2.0, d=D2)
        result = fetch_log.get_complete_cells(START, END)
        assert (1.0, 2.0) not in result

    def test_incomplete_when_one_date_has_failed_endpoint(self, fetch_log):
        _record(fetch_log, lat=1.0, lon=2.0, d=D1)
        _record(fetch_log, lat=1.0, lon=2.0, d=D2)
        _record(fetch_log, lat=1.0, lon=2.0, d=D3, hourly_ok=False)
        result = fetch_log.get_complete_cells(START, END)
        assert (1.0, 2.0) not in result

    def test_returns_set_type(self, fetch_log):
        result = fetch_log.get_complete_cells(START, END)
        assert isinstance(result, set)

    def test_empty_when_no_rows(self, fetch_log):
        assert fetch_log.get_complete_cells(START, END) == set()

    def test_single_day_window(self, fetch_log):
        _record(fetch_log, lat=1.0, lon=2.0, d=D1)
        result = fetch_log.get_complete_cells(D1, D1)
        assert (1.0, 2.0) in result

    def test_does_not_count_duplicate_dates(self, fetch_log):
        # Two rows for D1 via separate cells; only one cell covers the full window
        _record_window(fetch_log, lat=1.0, lon=2.0)
        _record(fetch_log, lat=3.0, lon=4.0, d=D1)  # only one date
        result = fetch_log.get_complete_cells(START, END)
        assert (1.0, 2.0) in result
        assert (3.0, 4.0) not in result


# ---------------------------------------------------------------------------
# get_missing_dates_per_cell()
# ---------------------------------------------------------------------------

class TestGetMissingDatesPerCell:

    def test_returns_missing_dates_for_partial_cell(self, fetch_log):
        _record(fetch_log, lat=1.0, lon=2.0, d=D1)
        result = fetch_log.get_missing_dates_per_cell(START, END)
        assert (1.0, 2.0) in result
        assert D2 in result[(1.0, 2.0)]
        assert D3 in result[(1.0, 2.0)]
        assert D1 not in result[(1.0, 2.0)]

    def test_missing_dates_are_sorted(self, fetch_log):
        _record(fetch_log, lat=1.0, lon=2.0, d=D2)
        missing = fetch_log.get_missing_dates_per_cell(START, END)[(1.0, 2.0)]
        assert missing == sorted(missing)

    def test_absent_cell_not_in_result(self, fetch_log):
        # Cell with no log entries at all → absent from result dict
        result = fetch_log.get_missing_dates_per_cell(START, END)
        assert (1.0, 2.0) not in result

    def test_fully_complete_cell_not_in_result(self, fetch_log):
        _record_window(fetch_log, lat=1.0, lon=2.0)
        result = fetch_log.get_missing_dates_per_cell(START, END)
        assert (1.0, 2.0) not in result

    def test_all_three_missing_when_only_middle_covered(self, fetch_log):
        _record(fetch_log, lat=1.0, lon=2.0, d=D2)
        missing = fetch_log.get_missing_dates_per_cell(START, END)[(1.0, 2.0)]
        assert D1 in missing
        assert D2 not in missing
        assert D3 in missing

    def test_empty_dict_when_no_rows(self, fetch_log):
        assert fetch_log.get_missing_dates_per_cell(START, END) == {}

    def test_ignores_incomplete_ok_rows(self, fetch_log):
        # A row where hourly_ok=0 is NOT counted as covered
        _record(fetch_log, lat=1.0, lon=2.0, d=D1, hourly_ok=False)
        result = fetch_log.get_missing_dates_per_cell(START, END)
        # D1 has a row but it's not complete, so this cell doesn't appear in result
        # (covered dict only builds from fully-ok rows)
        assert (1.0, 2.0) not in result


# ---------------------------------------------------------------------------
# prune()
# ---------------------------------------------------------------------------

class TestPrune:

    def test_deletes_rows_with_date_before_cutoff(self, fetch_log):
        _record(fetch_log, lat=1.0, lon=2.0, d=date(2024, 1, 1))
        _record(fetch_log, lat=3.0, lon=4.0, d=date(2025, 6, 1))

        deleted = fetch_log.prune(date(2025, 1, 1))
        assert deleted == 1

    def test_keeps_rows_at_or_after_cutoff(self, fetch_log, db):
        cutoff = date(2025, 1, 1)
        _record(fetch_log, lat=1.0, lon=2.0, d=cutoff)
        fetch_log.prune(cutoff)
        remaining = db.execute("SELECT COUNT(*) FROM weather_fetch_log").fetchone()[0]
        assert remaining == 1

    def test_returns_deleted_count(self, fetch_log):
        _record(fetch_log, lat=1.0, lon=2.0, d=date(2024, 1, 1))
        _record(fetch_log, lat=3.0, lon=4.0, d=date(2024, 2, 1))
        deleted = fetch_log.prune(date(2025, 1, 1))
        assert deleted == 2

    def test_returns_zero_when_nothing_to_prune(self, fetch_log):
        _record(fetch_log, lat=1.0, lon=2.0, d=date(2026, 1, 1))
        assert fetch_log.prune(date(2025, 1, 1)) == 0


# ---------------------------------------------------------------------------
# prune_by_retention()
# ---------------------------------------------------------------------------

class TestPruneByRetention:

    def test_deletes_rows_outside_retention_window(self, fetch_log):
        old_date = date.today() - timedelta(days=20)
        _record(fetch_log, lat=1.0, lon=2.0, d=old_date)
        deleted = fetch_log.prune_by_retention(retention_days=14)
        assert deleted == 1

    def test_keeps_rows_within_retention_window(self, fetch_log, db):
        recent_date = date.today() - timedelta(days=5)
        _record(fetch_log, lat=1.0, lon=2.0, d=recent_date)
        fetch_log.prune_by_retention(retention_days=14)
        remaining = db.execute("SELECT COUNT(*) FROM weather_fetch_log").fetchone()[0]
        assert remaining == 1


# ---------------------------------------------------------------------------
# _contiguous_ranges()
# ---------------------------------------------------------------------------

class TestContiguousRanges:

    def test_empty_list(self):
        assert _contiguous_ranges([]) == []

    def test_single_date(self):
        assert _contiguous_ranges([D1]) == [(D1, D1)]

    def test_two_consecutive_dates(self):
        assert _contiguous_ranges([D1, D2]) == [(D1, D2)]

    def test_three_consecutive_dates(self):
        assert _contiguous_ranges([D1, D2, D3]) == [(D1, D3)]

    def test_gap_splits_into_two_ranges(self):
        # D1, then skip D2, then D3
        result = _contiguous_ranges([D1, D3])
        assert result == [(D1, D1), (D3, D3)]

    def test_mixed_consecutive_and_gaps(self):
        d4 = D3 + timedelta(days=1)
        d6 = d4 + timedelta(days=2)
        # D1 D2 D3 (consecutive), D4 (consecutive with D3), gap, D6
        result = _contiguous_ranges([D1, D2, D3, d4, d6])
        assert result == [(D1, d4), (d6, d6)]
