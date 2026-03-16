"""
tests/test_notifications.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Unit tests for app/notifications.py.

Covers:
  - send_email: happy path, SMTP failure
  - CronJobMailer: success email, failure email, metric inclusion,
                   exception propagation, mail send failure not masking original exception
  - _format_duration: seconds / minutes / hours
  - _build_body: success and failure variants
"""

import smtplib
from datetime import datetime, timezone
from unittest.mock import MagicMock, call, patch

import pytest

from notifications import CronJobMailer, _build_body, _format_duration, send_email

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

SMTP_CFG = {
    "smtp_host": "smtp.example.com",
    "smtp_port": 587,
    "sender": "travelnet@example.com",
    "password": "secret",
    "recipient": "dan@example.com",
}

MAILER_SMTP_CFG = {
    "smtp_host": "smtp.example.com",
    "smtp_port": 587,
    "sender": "travelnet@example.com",
    "password": "secret",
    "recipient": "dan@example.com",
}


# ---------------------------------------------------------------------------
# send_email
# ---------------------------------------------------------------------------

class TestSendEmail:

    def test_sends_with_correct_credentials(self):
        """send_email logs in and sends via the SMTP context manager."""
        with patch("notifications.smtplib.SMTP") as mock_smtp_cls:
            mock_smtp = MagicMock()
            mock_smtp_cls.return_value.__enter__.return_value = mock_smtp

            send_email(
                subject="Test subject",
                body="Test body",
                **SMTP_CFG,
            )

        mock_smtp_cls.assert_called_once_with("smtp.example.com", 587)
        mock_smtp.starttls.assert_called_once()
        mock_smtp.login.assert_called_once_with("travelnet@example.com", "secret")
        mock_smtp.send_message.assert_called_once()

    def test_sent_message_has_correct_headers(self):
        """The EmailMessage passed to send_message has the right Subject/From/To."""
        with patch("notifications.smtplib.SMTP") as mock_smtp_cls:
            mock_smtp = MagicMock()
            mock_smtp_cls.return_value.__enter__.return_value = mock_smtp

            send_email(
                subject="Hello TravelNet",
                body="Body text",
                **SMTP_CFG,
            )

        sent_msg = mock_smtp.send_message.call_args[0][0]
        assert sent_msg["Subject"] == "Hello TravelNet"
        assert sent_msg["From"] == "travelnet@example.com"
        assert sent_msg["To"] == "dan@example.com"

    def test_raises_on_smtp_failure(self):
        """send_email propagates SMTP exceptions to the caller."""
        with patch("notifications.smtplib.SMTP") as mock_smtp_cls:
            mock_smtp_cls.side_effect = smtplib.SMTPException("connection refused")

            with pytest.raises(smtplib.SMTPException, match="connection refused"):
                send_email(subject="x", body="y", **SMTP_CFG)


# ---------------------------------------------------------------------------
# CronJobMailer — success path
# ---------------------------------------------------------------------------

class TestCronJobMailerSuccess:

    def test_sends_success_email_on_clean_exit(self):
        """A clean with-block triggers exactly one send_email call."""
        with patch("notifications.send_email") as mock_send:
            with CronJobMailer("test_job", MAILER_SMTP_CFG):
                pass

        mock_send.assert_called_once()

    def test_success_subject_contains_checkmark_and_job_name(self):
        with patch("notifications.send_email") as mock_send:
            with CronJobMailer("get_fx", MAILER_SMTP_CFG):
                pass

        subject = mock_send.call_args.kwargs["subject"]
        assert "✅" in subject
        assert "get_fx" in subject

    def test_success_subject_does_not_contain_failed(self):
        with patch("notifications.send_email") as mock_send:
            with CronJobMailer("get_fx", MAILER_SMTP_CFG):
                pass

        subject = mock_send.call_args.kwargs["subject"]
        assert "FAILED" not in subject
        assert "❌" not in subject

    def test_metrics_appear_in_success_body(self):
        with patch("notifications.send_email") as mock_send:
            with CronJobMailer("get_fx", MAILER_SMTP_CFG) as job:
                job.add_metric("dates inserted", 28)
                job.add_metric("month", "2026-02")

        body = mock_send.call_args.kwargs["body"]
        assert "dates inserted" in body
        assert "28" in body
        assert "month" in body
        assert "2026-02" in body

    def test_body_contains_job_name_and_status(self):
        with patch("notifications.send_email") as mock_send:
            with CronJobMailer("backfill_amount_gbp", MAILER_SMTP_CFG):
                pass

        body = mock_send.call_args.kwargs["body"]
        assert "backfill_amount_gbp" in body
        assert "SUCCESS" in body

    def test_body_contains_duration(self):
        with patch("notifications.send_email") as mock_send:
            with CronJobMailer("get_fx", MAILER_SMTP_CFG):
                pass

        body = mock_send.call_args.kwargs["body"]
        assert "Duration" in body

    def test_correct_smtp_kwargs_passed_on_success(self):
        """send_email is called with the smtp_cfg values unpacked correctly."""
        with patch("notifications.send_email") as mock_send:
            with CronJobMailer("get_fx", MAILER_SMTP_CFG):
                pass

        kwargs = mock_send.call_args.kwargs
        assert kwargs["smtp_host"] == "smtp.example.com"
        assert kwargs["smtp_port"] == 587
        assert kwargs["sender"] == "travelnet@example.com"
        assert kwargs["password"] == "secret"
        assert kwargs["recipient"] == "dan@example.com"


# ---------------------------------------------------------------------------
# CronJobMailer — failure path
# ---------------------------------------------------------------------------

class TestCronJobMailerFailure:

    def test_sends_failure_email_on_exception(self):
        """An exception inside the with-block triggers exactly one send_email call."""
        with patch("notifications.send_email") as mock_send:
            with pytest.raises(RuntimeError):
                with CronJobMailer("get_fx", MAILER_SMTP_CFG):
                    raise RuntimeError("API error")

        mock_send.assert_called_once()

    def test_failure_subject_contains_cross_and_failed(self):
        with patch("notifications.send_email") as mock_send:
            with pytest.raises(ValueError):
                with CronJobMailer("get_fx", MAILER_SMTP_CFG):
                    raise ValueError("bad value")

        subject = mock_send.call_args.kwargs["subject"]
        assert "❌" in subject
        assert "FAILED" in subject
        assert "get_fx" in subject

    def test_failure_subject_does_not_contain_checkmark(self):
        with patch("notifications.send_email") as mock_send:
            with pytest.raises(Exception):
                with CronJobMailer("get_fx", MAILER_SMTP_CFG):
                    raise Exception("oops")

        subject = mock_send.call_args.kwargs["subject"]
        assert "✅" not in subject

    def test_traceback_appears_in_failure_body(self):
        with patch("notifications.send_email") as mock_send:
            with pytest.raises(RuntimeError):
                with CronJobMailer("get_fx", MAILER_SMTP_CFG):
                    raise RuntimeError("something went wrong")

        body = mock_send.call_args.kwargs["body"]
        assert "Traceback" in body
        assert "something went wrong" in body

    def test_metrics_logged_before_failure_appear_in_body(self):
        """Metrics added before the exception are still included in the failure email."""
        with patch("notifications.send_email") as mock_send:
            with pytest.raises(RuntimeError):
                with CronJobMailer("get_fx", MAILER_SMTP_CFG) as job:
                    job.add_metric("dates inserted", 14)
                    raise RuntimeError("failed mid-job")

        body = mock_send.call_args.kwargs["body"]
        assert "dates inserted" in body
        assert "14" in body

    def test_original_exception_is_not_suppressed(self):
        """CronJobMailer must never swallow the original exception."""
        with patch("notifications.send_email"):
            with pytest.raises(RuntimeError, match="do not suppress me"):
                with CronJobMailer("get_fx", MAILER_SMTP_CFG):
                    raise RuntimeError("do not suppress me")

    def test_mail_failure_does_not_mask_original_exception(self):
        """If send_email itself raises, the original exception still propagates."""
        with patch("notifications.send_email", side_effect=smtplib.SMTPException("mail down")):
            with pytest.raises(RuntimeError, match="original error"):
                with CronJobMailer("get_fx", MAILER_SMTP_CFG):
                    raise RuntimeError("original error")

    def test_mail_failure_on_success_path_does_not_raise(self):
        """If send_email raises on a clean exit, CronJobMailer swallows it silently."""
        with patch("notifications.send_email", side_effect=smtplib.SMTPException("mail down")):
            # Should not raise — the job itself succeeded
            with CronJobMailer("get_fx", MAILER_SMTP_CFG):
                pass


# ---------------------------------------------------------------------------
# _format_duration
# ---------------------------------------------------------------------------

class TestFormatDuration:

    @pytest.mark.parametrize("seconds, expected", [
        (0,    "0s"),
        (1,    "1s"),
        (59,   "59s"),
        (60,   "1m 0s"),
        (61,   "1m 1s"),
        (90,   "1m 30s"),
        (3599, "59m 59s"),
        (3600, "1h 0m 0s"),
        (3661, "1h 1m 1s"),
        (7322, "2h 2m 2s"),
    ])
    def test_format(self, seconds, expected):
        assert _format_duration(seconds) == expected

    def test_fractional_seconds_are_truncated(self):
        """Float input should be truncated, not rounded."""
        assert _format_duration(61.9) == "1m 1s"


# ---------------------------------------------------------------------------
# _build_body
# ---------------------------------------------------------------------------

class TestBuildBody:

    def test_success_body_contains_required_fields(self):
        body = _build_body(
            job_name="get_fx",
            status="SUCCESS",
            started="2026-03-02 02:00:01 UTC",
            finished="2026-03-02 02:00:03 UTC",
            duration="2s",
            metrics=[],
        )
        assert "get_fx" in body
        assert "SUCCESS" in body
        assert "2026-03-02 02:00:01 UTC" in body
        assert "2026-03-02 02:00:03 UTC" in body
        assert "2s" in body

    def test_failure_body_contains_error_section(self):
        body = _build_body(
            job_name="get_fx",
            status="FAILED",
            started="2026-03-02 02:00:01 UTC",
            finished="2026-03-02 02:00:02 UTC",
            duration="1s",
            metrics=[],
            error="Traceback (most recent call last):\n  ...\nRuntimeError: boom",
        )
        assert "FAILED" in body
        assert "Traceback" in body
        assert "RuntimeError: boom" in body

    def test_metrics_section_absent_when_empty(self):
        body = _build_body(
            job_name="get_fx",
            status="SUCCESS",
            started="2026-03-02 02:00:01 UTC",
            finished="2026-03-02 02:00:02 UTC",
            duration="1s",
            metrics=[],
        )
        assert "Metrics:" not in body

    def test_metrics_section_present_when_provided(self):
        body = _build_body(
            job_name="get_fx",
            status="SUCCESS",
            started="2026-03-02 02:00:01 UTC",
            finished="2026-03-02 02:00:02 UTC",
            duration="1s",
            metrics=[("dates inserted", 28), ("month", "2026-02")],
        )
        assert "Metrics:" in body
        assert "dates inserted" in body
        assert "28" in body
        assert "2026-02" in body

    def test_no_error_section_in_success_body(self):
        body = _build_body(
            job_name="get_fx",
            status="SUCCESS",
            started="2026-03-02 02:00:01 UTC",
            finished="2026-03-02 02:00:02 UTC",
            duration="1s",
            metrics=[],
        )
        assert "Traceback" not in body