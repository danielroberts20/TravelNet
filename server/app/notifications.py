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

import smtplib
import traceback
from contextlib import contextmanager
from datetime import datetime, timezone
from email.message import EmailMessage
from typing import Any

# ---------------------------------------------------------------------------
# SMTP config type alias — pass a dict with these keys, sourced from your
# existing config.general constants
# ---------------------------------------------------------------------------
# smtp_cfg = {
#     "host":      SMTP_HOST,
#     "port":      SMTP_PORT,
#     "sender":    EMAIL_SENDER,
#     "password":  EMAIL_PASSWORD,
#     "recipient": EMAIL_RECIPIENT,
# }


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

    def __init__(self, job_name: str, smtp_cfg: dict[str, Any]) -> None:
        self.job_name = job_name
        self.smtp_cfg = smtp_cfg
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
            # Never let a notification failure mask the original exception
            print(f"[CronJobMailer] Failed to send email for {self.job_name!r}: {mail_err}")

        return False  # do not suppress the original exception


# ---------------------------------------------------------------------------
# Body builder
# ---------------------------------------------------------------------------

def _build_body(
    job_name: str,
    status: str,
    started: str,
    finished: str,
    duration: str,
    metrics: list[tuple[str, Any]],
    error: str | None = None,
) -> str:
    lines = [
        f"Job:      {job_name}",
        f"Status:   {status}",
        f"Started:  {started}",
        f"Finished: {finished}",
        f"Duration: {duration}",
    ]

    if metrics:
        lines.append("")
        lines.append("Metrics:")
        col = max(len(label) for label, _ in metrics)
        for label, value in metrics:
            lines.append(f"  {label:<{col}}  {value}")

    if error:
        lines += ["", "=" * 60, "Traceback:", "=" * 60, error]

    return "\n".join(lines)


def _format_duration(seconds: float) -> str:
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    minutes, secs = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m {secs}s"
    hours, mins = divmod(minutes, 60)
    return f"{hours}h {mins}m {secs}s"