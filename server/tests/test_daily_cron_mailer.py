"""
test_daily_cron_mailer.py — Unit tests for DailyCronJobMailer.__exit__().

Covers:
  - Record is inserted via daily_cron_table.insert on clean exit
  - Record is inserted via daily_cron_table.insert on exception exit
  - success=True written on clean exit, success=False on exception exit
  - duration_s is a non-negative float
  - metrics serialised as JSON when add_metric() called
  - metrics is None when no metrics added
  - error text captured on exception exit, None on clean exit
  - _flush_and_send called when all DAILY_CRON_JOBS have reported
  - _flush_and_send NOT called when some jobs haven't reported yet
  - Exception from the wrapped block is NOT suppressed
"""

import json
import sqlite3
import pytest
from unittest.mock import patch, MagicMock, call

from notifications import DailyCronJobMailer
from database.logging.daily.table import DailyCronRecord


SMTP = {"smtp_host": "h", "smtp_port": 25, "username": "u", "sender": "s@s", "password": "p", "recipient": "r@r"}


def _make_db(reported_jobs, date="2026-04-11"):
    """Return an in-memory DB with cron_results rows for the given jobs."""
    conn = sqlite3.connect(":memory:")
    conn.execute("""
        CREATE TABLE cron_results (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            job_name  TEXT NOT NULL,
            ran_at    TEXT NOT NULL,
            date      TEXT NOT NULL,
            success   INTEGER NOT NULL,
            duration_s REAL,
            metrics   TEXT,
            error     TEXT
        )
    """)
    for job in reported_jobs:
        conn.execute(
            "INSERT INTO cron_results (job_name, ran_at, date, success) VALUES (?, ?, ?, 1)",
            (job, "2026-04-11T12:00:00Z", date),
        )
    conn.commit()
    return conn


def _run(job_name="test_job", raise_exc=None, metrics=None, smtp=None,
         all_jobs=("test_job",), db_reported=None):
    """
    Run DailyCronJobMailer as a context manager.

    all_jobs:    value to patch DAILY_CRON_JOBS with
    db_reported: jobs present in cron_results (defaults to all_jobs)
    """
    if smtp is None:
        smtp = SMTP
    if db_reported is None:
        db_reported = list(all_jobs)

    db = _make_db(db_reported)

    inserted_records = []

    def fake_insert(record):
        inserted_records.append(record)

    with patch("notifications.daily_cron_table") as mock_tbl, \
         patch("notifications.get_conn", return_value=db), \
         patch("notifications._flush_and_send") as mock_flush, \
         patch("notifications.DAILY_CRON_JOBS", list(all_jobs)), \
         patch("notifications._record_cron_run"):  # CronJobMailer base may call this

        mock_tbl.insert.side_effect = fake_insert

        # Use the real `with` block so that traceback.format_exc() works inside __exit__
        try:
            with DailyCronJobMailer(job_name, smtp) as mailer:
                if metrics:
                    for label, value in metrics:
                        mailer.add_metric(label, value)
                if raise_exc:
                    raise raise_exc
        except Exception:
            pass  # DailyCronJobMailer.__exit__ returns False, so exception propagates

    return inserted_records, mock_tbl, mock_flush


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestDailyCronJobMailer:

    def test_insert_called_on_clean_exit(self):
        records, mock_tbl, _ = _run()
        mock_tbl.insert.assert_called_once()

    def test_insert_called_on_exception_exit(self):
        records, mock_tbl, _ = _run(raise_exc=RuntimeError("oops"))
        mock_tbl.insert.assert_called_once()

    def test_success_true_on_clean_exit(self):
        records, _, _ = _run()
        assert records[0].success is True

    def test_success_false_on_exception_exit(self):
        records, _, _ = _run(raise_exc=ValueError("fail"))
        assert records[0].success is False

    def test_duration_s_is_non_negative(self):
        records, _, _ = _run()
        assert records[0].duration_s >= 0

    def test_metrics_serialised_as_json(self):
        records, _, _ = _run(metrics=[("inserted", 5), ("skipped", 1)])
        parsed = json.loads(records[0].metrics)
        assert parsed["inserted"] == 5
        assert parsed["skipped"] == 1

    def test_metrics_none_when_none_added(self):
        records, _, _ = _run()
        assert records[0].metrics is None

    def test_error_none_on_clean_exit(self):
        records, _, _ = _run()
        assert records[0].error is None

    def test_error_captured_on_exception_exit(self):
        records, _, _ = _run(raise_exc=RuntimeError("boom"))
        assert records[0].error is not None
        assert "RuntimeError" in records[0].error

    def test_job_name_stored(self):
        records, _, _ = _run(job_name="my_cron_job")
        assert records[0].job_name == "my_cron_job"

    def test_flush_called_when_all_jobs_reported(self):
        _, _, mock_flush = _run(
            all_jobs=("job_a", "job_b"),
            db_reported=["job_a", "job_b"],
        )
        mock_flush.assert_called_once()

    def test_flush_not_called_when_jobs_missing(self):
        _, _, mock_flush = _run(
            all_jobs=("job_a", "job_b"),
            db_reported=["job_a"],  # job_b hasn't reported yet
        )
        mock_flush.assert_not_called()

    def test_flush_not_called_when_no_jobs_reported(self):
        _, _, mock_flush = _run(
            all_jobs=("job_a",),
            db_reported=[],
        )
        mock_flush.assert_not_called()

    def test_flush_receives_smtp_cfg(self):
        _, _, mock_flush = _run(
            smtp=SMTP,
            all_jobs=("test_job",),
            db_reported=["test_job"],
        )
        mock_flush.assert_called_once_with(SMTP)

    def test_exception_not_suppressed(self):
        with patch("notifications.daily_cron_table"), \
             patch("notifications.get_conn", return_value=_make_db([])), \
             patch("notifications._flush_and_send"), \
             patch("notifications.DAILY_CRON_JOBS", []):
            with pytest.raises(ValueError, match="err"):
                with DailyCronJobMailer("j", SMTP):
                    raise ValueError("err")
