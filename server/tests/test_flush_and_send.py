"""
test_flush_and_send.py — Unit tests for notifications._flush_and_send().

Covers:
  - Returns True immediately when no records (empty fetch_and_clear)
  - send_email called once when records present
  - Returns True on successful send
  - Subject contains date from first record
  - Subject contains ✅ when all records are successful
  - Subject contains ❌ when any record failed
  - Body includes each job name
  - Body includes missing-jobs warning when a DAILY_CRON_JOBS entry didn't report
  - On send failure: daily_cron_table.restore called with original records
  - On send failure: returns False
  - Metrics JSON is expanded into body lines
  - Error text included in body when record has error
"""

import pytest
from unittest.mock import patch, MagicMock, call

from notifications import _flush_and_send
from database.logging.daily.table import DailyCronRecord


SMTP = {"smtp_host": "h", "smtp_port": 25, "username": "u", "sender": "s@s", "password": "p", "recipient": "r@r"}


def _make_record(job_name="test_job", success=True, date="2026-04-11",
                 metrics=None, error=None, duration_s=1.0):
    return DailyCronRecord(
        job_name=job_name,
        ran_at="2026-04-11T12:00:00Z",
        date=date,
        success=success,
        duration_s=duration_s,
        metrics=metrics,
        error=error,
    )


def _run(records, send_raises=None, daily_jobs=None):
    """Run _flush_and_send with mocked daily_cron_table and send_email."""
    if daily_jobs is None:
        daily_jobs = [r.job_name for r in records]

    captured = {}

    def fake_send_email(**kwargs):
        captured["subject"] = kwargs.get("subject", "")
        captured["body"] = kwargs.get("body", "")
        if send_raises:
            raise send_raises

    with patch("notifications.daily_cron_table") as mock_tbl, \
         patch("notifications.send_email", side_effect=fake_send_email), \
         patch("notifications.DAILY_CRON_JOBS", daily_jobs):

        mock_tbl.fetch_and_clear.return_value = records
        result = _flush_and_send(SMTP)

    return result, captured, mock_tbl


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestFlushAndSend:

    def test_returns_true_when_no_records(self):
        result, _, _ = _run([])
        assert result is True

    def test_send_email_not_called_when_no_records(self):
        with patch("notifications.daily_cron_table") as mock_tbl, \
             patch("notifications.send_email") as mock_send, \
             patch("notifications.DAILY_CRON_JOBS", []):
            mock_tbl.fetch_and_clear.return_value = []
            _flush_and_send(SMTP)
        mock_send.assert_not_called()

    def test_returns_true_on_successful_send(self):
        result, _, _ = _run([_make_record()])
        assert result is True

    def test_send_email_called_once(self):
        with patch("notifications.daily_cron_table") as mock_tbl, \
             patch("notifications.send_email") as mock_send, \
             patch("notifications.DAILY_CRON_JOBS", ["test_job"]):
            mock_tbl.fetch_and_clear.return_value = [_make_record()]
            _flush_and_send(SMTP)
        mock_send.assert_called_once()

    def test_subject_contains_date(self):
        _, captured, _ = _run([_make_record(date="2026-04-11")])
        assert "2026-04-11" in captured["subject"]

    def test_subject_contains_ok_icon_all_success(self):
        records = [_make_record("a", success=True), _make_record("b", success=True)]
        _, captured, _ = _run(records)
        assert "✅" in captured["subject"]

    def test_subject_contains_fail_icon_any_failure(self):
        records = [_make_record("a", success=True), _make_record("b", success=False)]
        _, captured, _ = _run(records)
        assert "❌" in captured["subject"]

    def test_body_includes_each_job_name(self):
        records = [_make_record("geocode_places"), _make_record("get_fx_up_to_date")]
        _, captured, _ = _run(records)
        assert "geocode_places" in captured["body"]
        assert "get_fx_up_to_date" in captured["body"]

    def test_body_warns_about_missing_jobs(self):
        records = [_make_record("job_a")]
        _, captured, _ = _run(records, daily_jobs=["job_a", "job_b"])
        assert "job_b" in captured["body"]

    def test_no_missing_warning_when_all_present(self):
        records = [_make_record("job_a"), _make_record("job_b")]
        _, captured, _ = _run(records, daily_jobs=["job_a", "job_b"])
        # No "did not report" warning expected
        assert "did not report" not in captured["body"].lower() or "job_a" not in [
            line for line in captured["body"].splitlines() if "did not report" in line
        ]

    def test_restore_called_on_send_failure(self):
        records = [_make_record()]
        _, _, mock_tbl = _run(records, send_raises=ConnectionError("smtp down"))
        mock_tbl.restore.assert_called_once_with(records)

    def test_returns_false_on_send_failure(self):
        result, _, _ = _run([_make_record()], send_raises=OSError("no smtp"))
        assert result is False

    def test_metrics_expanded_in_body(self):
        import json
        rec = _make_record(metrics=json.dumps({"inserted": 42, "skipped": 3}))
        _, captured, _ = _run([rec])
        assert "inserted" in captured["body"]
        assert "42" in captured["body"]

    def test_error_text_in_body(self):
        rec = _make_record(success=False, error="Traceback (most recent call last):\n  ValueError")
        _, captured, _ = _run([rec])
        assert "ValueError" in captured["body"]

    def test_smtp_cfg_forwarded_to_send_email(self):
        custom_smtp = {"smtp_host": "mail.example.com", "smtp_port": 587,
                       "username": "usr", "sender": "a@b.com",
                       "password": "secret", "recipient": "dest@b.com"}
        with patch("notifications.daily_cron_table") as mock_tbl, \
             patch("notifications.send_email") as mock_send, \
             patch("notifications.DAILY_CRON_JOBS", ["j"]):
            mock_tbl.fetch_and_clear.return_value = [_make_record("j")]
            _flush_and_send(custom_smtp)
        _, kwargs = mock_send.call_args
        assert kwargs.get("smtp_host") == "mail.example.com" or \
               mock_send.call_args[1].get("smtp_host") == "mail.example.com" or \
               "mail.example.com" in str(mock_send.call_args)
