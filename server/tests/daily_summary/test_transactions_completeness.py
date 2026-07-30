"""
test_transactions_completeness.py — Tests for the spend_complete logic in
scheduled_tasks/daily_summary/transactions.py.

Previously spend_complete was set unconditionally by the monthly backfill
flow once it finished running, regardless of whether an upload for that
month had ever happened — a month nobody uploaded and a genuine zero-spend
month were indistinguishable, and both got marked complete. It should now
be derived from upload_log: complete only once every required source has
an upload covering the date's calendar month.
"""

import sqlite3

import pytest

from scheduled_tasks.daily_summary.transactions import (
    REQUIRED_UPLOAD_SOURCES,
    _upload_coverage_predicate,
)

UPLOAD_LOG_DDL = """
CREATE TABLE upload_log (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    source        TEXT NOT NULL,
    period_start  TEXT NOT NULL,
    period_end    TEXT NOT NULL,
    row_count     INTEGER NOT NULL,
    inferred      INTEGER NOT NULL DEFAULT 0,
    uploaded_at   TEXT
);
"""


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    c.executescript(UPLOAD_LOG_DDL)
    return c


def _add_upload(conn, source, period_start, period_end, row_count=1):
    conn.execute(
        "INSERT INTO upload_log (source, period_start, period_end, row_count) VALUES (?, ?, ?, ?)",
        (source, period_start, period_end, row_count),
    )


def test_required_sources_are_revolut_and_wise():
    assert set(REQUIRED_UPLOAD_SOURCES) == {"revolut", "wise"}


def test_incomplete_with_no_uploads(conn):
    assert _upload_coverage_predicate("2026-03-15", {}, conn) is False


def test_incomplete_when_only_one_source_covers(conn):
    _add_upload(conn, "revolut", "2026-03-01", "2026-03-31")
    assert _upload_coverage_predicate("2026-03-15", {}, conn) is False


def test_complete_when_both_sources_cover(conn):
    _add_upload(conn, "revolut", "2026-03-01", "2026-03-31")
    _add_upload(conn, "wise", "2026-03-01", "2026-03-31")
    assert _upload_coverage_predicate("2026-03-15", {}, conn) is True


def test_date_outside_any_uploaded_period_is_incomplete(conn):
    _add_upload(conn, "revolut", "2026-03-01", "2026-03-31")
    _add_upload(conn, "wise", "2026-03-01", "2026-03-31")
    assert _upload_coverage_predicate("2026-04-01", {}, conn) is False


def test_zero_row_count_upload_still_counts_as_coverage(conn):
    """A confirmed-empty upload (row_count = 0) still proves the month was
    checked — distinguishing a genuine zero-spend month from one nobody
    uploaded yet."""
    _add_upload(conn, "revolut", "2026-03-01", "2026-03-31", row_count=0)
    _add_upload(conn, "wise", "2026-03-01", "2026-03-31", row_count=0)
    assert _upload_coverage_predicate("2026-03-15", {}, conn) is True


def test_boundary_dates_are_covered(conn):
    _add_upload(conn, "revolut", "2026-03-01", "2026-03-31")
    _add_upload(conn, "wise", "2026-03-01", "2026-03-31")
    assert _upload_coverage_predicate("2026-03-01", {}, conn) is True
    assert _upload_coverage_predicate("2026-03-31", {}, conn) is True
