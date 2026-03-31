"""
notifications.py
~~~~~~~~~~~~~~~~
Centralised email utilities for TravelNet.

Provides:
  - send_email()       — low-level SMTP send, used by everything below
  - CronJobMailer      — context manager that wraps a cron job and sends a
                         completion (✅) or failure (❌) email automatically
  - Updated flush logic for DailyDigestHandler (see refactor note at bottom)
"""
import logging
import smtplib
import traceback
from contextlib import contextmanager
from config.general import AVAILABLE_NOTIFICATIONS
from config.settings import settings
from datetime import datetime, timezone
from email.message import EmailMessage
from typing import Any

import requests

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# PushCut
# ---------------------------------------------------------------------------
def trigger_notification(notification_name):
    """Fire a named Pushcut notification by looking up its URL from AVAILABLE_NOTIFICATIONS."""
    noti_url = AVAILABLE_NOTIFICATIONS.get(notification_name)
    if noti_url:
        resp = requests.post(noti_url)
        logger.info(resp)
        return {}

def send_notification(title: str = "Title", body: str = "Body", time_sensitive: bool = True, use_prefix: bool = True, prefix: str = None):
    """Send a Pushcut push notification via the custom webhook.

    :param title: notification title (prefixed with 'TravelNet — ' unless use_prefix=False).
    :param body: notification body text.
    :param time_sensitive: if True, uses the time-sensitive webhook (breaks through Focus).
    :param use_prefix: prepend 'TravelNet — ' to the title when True.
    """
    url = settings.custom_notification_time_sensitive if time_sensitive else settings.custom_notification_not_time_sensitive
    return _trigger_notification(url, title, body, use_prefix, prefix)

def journal_notification(title: str = "Title", body: str = "Body", time_sensitive: bool = True, use_prefix: bool = True, prefix: str = None):
    url = settings.journal_notification
    return _trigger_notification(url, title, body, use_prefix, prefix)

def _trigger_notification(url, title: str = "Title", body: str = "Body", use_prefix: bool = True, prefix: str = None):
    if use_prefix:
        title = f"TravelNet — {title}" if prefix is None else f"{prefix} — {title}"
    payload = {
        "text": body,
        "title": title
    }

    headers = {
        "Content-Type": "application/json"
    }
    resp = requests.post(url, json=payload, headers=headers)
    return resp.json()

def warning_notification(body: str = "Warning"):
    """Send a warning-level notification to the dedicated warning webhook."""
    _warn_error_notification(body, is_warn=True)

def error_notification(body: str = "Error"):
    """Send an error-level notification to the dedicated error webhook."""
    _warn_error_notification(body, is_warn=False)

def _warn_error_notification(body: str, is_warn: bool):
    """Internal helper: POST body text to the warn or error Pushcut webhook."""
    payload = {
        "text": body
    }
    headers = {
        "Content-Type": "application/json"
    }
    url = settings.warning_notification if is_warn else settings.error_notification
    resp = requests.post(url, json=payload, headers=headers)
    return resp.json()

# ---------------------------------------------------------------------------
# Primitive
# ---------------------------------------------------------------------------

def send_email(
    subject: str,
    body: str,
    smtp_host: str,
    smtp_port: int,
    sender: str,
    password: str,
    recipient: str,
) -> None:
    """Send a plain-text email via SMTP with STARTTLS.

    Raises on failure — callers decide whether to swallow or re-raise.
    """
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = recipient
    msg.set_content(body)

    with smtplib.SMTP(smtp_host, smtp_port) as smtp:
        smtp.starttls()
        smtp.login(sender, password)
        smtp.send_message(msg)


# ---------------------------------------------------------------------------
# CronJobMailer
# ---------------------------------------------------------------------------

class CronJobMailer:
    """Context manager that sends a completion or failure email for a cron job.

    Usage::

        smtp_cfg = {
            "host": SMTP_HOST, "port": SMTP_PORT,
            "sender": EMAIL_SENDER, "password": EMAIL_PASSWORD,
            "recipient": EMAIL_RECIPIENT,
        }

        with CronJobMailer("get_fx_up_to_date", smtp_cfg) as job:
            result = run_fx_update()
            job.add_metric("rates inserted", result.inserted)
            job.add_metric("dates covered", result.date_range)

    On clean exit  → sends a ✅ subject with duration and any metrics.
    On exception   → sends a ❌ subject with duration, metrics so far,
                     and the full traceback. The exception is NOT suppressed.
    """

    def __init__(self, job_name: str, smtp_cfg: dict[str, Any], detail: str = "") -> None:
        self.job_name = job_name
        self.smtp_cfg = smtp_cfg
        self.detail = detail
        self._metrics: list[tuple[str, Any]] = []
        self._started_at: datetime | None = None

    # ------------------------------------------------------------------
    # Public API — call inside the `with` block
    # ------------------------------------------------------------------

    def add_metric(self, label: str, value: Any) -> None:
        """Record a key/value metric to include in the completion email."""
        self._metrics.append((label, value))

    # ------------------------------------------------------------------
    # Context manager protocol
    # ------------------------------------------------------------------

    def __enter__(self) -> "CronJobMailer":
        self._started_at = datetime.now(tz=timezone.utc)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        finished_at = datetime.now(tz=timezone.utc)
        duration = finished_at - self._started_at
        duration_str = _format_duration(duration.total_seconds())

        started_str  = self._started_at.strftime("%Y-%m-%d %H:%M:%S UTC")
        finished_str = finished_at.strftime("%Y-%m-%d %H:%M:%S UTC")

        if exc_type is None:
            subject = f"[TravelNet] ✅ {self.job_name} completed"
            body = _build_body(
                job_name=self.job_name,
                detail=self.detail,
                status="SUCCESS",
                started=started_str,
                finished=finished_str,
                duration=duration_str,
                metrics=self._metrics,
            )
        else:
            subject = f"[TravelNet] ❌ {self.job_name} FAILED"
            tb = traceback.format_exc()
            body = _build_body(
                job_name=self.job_name,
                detail=self.detail,
                status="FAILED",
                started=started_str,
                finished=finished_str,
                duration=duration_str,
                metrics=self._metrics,
                error=tb,
            )

        try:
            send_email(subject=subject, body=body, **self.smtp_cfg)
        except Exception as mail_err:
            print(f"[CronJobMailer] Failed to send email for {self.job_name!r}: {mail_err}")

        try:
            _record_cron_run(
                job=self.job_name,
                success=exc_type is None,
                detail=", ".join(f"{k}: {v}" for k, v in self._metrics) if self._metrics else "",
            )
        except Exception as tracker_err:
            print(f"[CronJobMailer] Failed to record cron run for {self.job_name!r}: {tracker_err}")

        return False  # do not suppress the original exception

# ---------------------------------------------------------------------------
# Body builder
# ---------------------------------------------------------------------------

def _build_body(
    job_name: str,
    detail: str,
    status: str,
    started: str,
    finished: str,
    duration: str,
    metrics: list[tuple[str, Any]],
    error: str | None = None,
) -> str:
    """Build the plain-text email body for a cron job completion or failure."""
    lines = [
        f"Job:      {job_name}",
        f"Status:   {status}",
        f"Started:  {started}",
        f"Finished: {finished}",
        f"Duration: {duration}",
        f"Detail:   {detail}"
    ]

    if metrics:
        lines.append("")
        lines.append("Metrics:")
        col = max(len(label) for label, _ in metrics)
        for label, value in metrics:
            lines.append(f"  {label:<{col}}  {value}")

    if error:
        lines += ["", "=" * 58, "Traceback:", "=" * 58, error]

    return "\n".join(lines)


def _format_duration(seconds: float) -> str:
    """Format a duration in seconds as a human-readable string (e.g. '2m 5s', '1h 3m 0s')."""
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    minutes, secs = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m {secs}s"
    hours, mins = divmod(minutes, 60)
    return f"{hours}h {mins}m {secs}s"

def _record_cron_run(job: str, success: bool, detail: str = "") -> None:
    """Write last-run status to /data/cron_runs.json for the dashboard."""
    import json
    import time
    from config.general import DATA_DIR

    path = DATA_DIR / "cron_runs.json"
    try:
        with open(path) as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        data = {}

    data[job] = {
        "success":   success,
        "detail":    detail,
        "timestamp": int(time.time()),
        "ts_human":  time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
    }

    tmp = path.with_suffix(".tmp")
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
    tmp.replace(path)
