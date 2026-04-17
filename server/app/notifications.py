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
import json
import logging
import smtplib
import traceback
from contextlib import contextmanager

from prefect import State
from database.connection import get_conn
from database.logging.daily.table import table as daily_cron_table, DailyCronRecord
from config.general import AVAILABLE_NOTIFICATIONS, DAILY_CRON_JOBS
from config.settings import settings
from datetime import datetime, timezone
from email.message import EmailMessage
from typing import Any

import requests

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# PushCut
# ---------------------------------------------------------------------------

def record_flow_result(result: dict) -> None:
    """Write a flow's return value to /data/flow_results.json for dashboard display.

    Reads the current Prefect run context for flow name and run ID automatically.
    Uses an exclusive flock + atomic rename so concurrent writers never corrupt the file,
    even though flows are not expected to overlap.
    """
    import fcntl
    import os
    import json as _json
    from config.general import DATA_DIR
    from prefect.context import get_run_context

    if not isinstance(result, dict):
        return

    try:
        ctx = get_run_context()
        flow_name   = ctx.flow.name
        flow_run_id = str(ctx.flow_run.id)
        # Skip sub-flows (called from within another flow) — they have no deployment_id
        # and would overwrite the standalone deployment run entry with an untracked run ID.
        if ctx.flow_run.deployment_id is None:
            return
    except Exception:
        return  # Not in a flow run context — skip silently

    path      = DATA_DIR / "flow_results.json"
    lock_path = DATA_DIR / "flow_results.lock"
    tmp_path  = DATA_DIR / "flow_results.tmp"

    try:
        with open(lock_path, "w") as _lock:
            fcntl.flock(_lock, fcntl.LOCK_EX)
            try:
                try:
                    with open(path) as f:
                        data = _json.load(f)
                except (FileNotFoundError, _json.JSONDecodeError):
                    data = {}

                data[flow_name] = {
                    "flow_run_id":  flow_run_id,
                    "result":       result,
                    "completed_at": datetime.now(timezone.utc).isoformat(),
                }

                with open(tmp_path, "w") as f:
                    _json.dump(data, f, indent=2, default=str)
                os.replace(tmp_path, path)
            finally:
                fcntl.flock(_lock, fcntl.LOCK_UN)
    except Exception:
        pass  # Never let result recording affect the flow run


def notify_on_completion(flow, flow_run, state: State):
    from notifications import send_notification

    send_notification(
        title=f"⏳ {flow.name}",
        body=state.message or ("✅ Completed successfully" if state.is_completed() else "❌ Failed"),
        time_sensitive=state.is_failed()
    )



    
def trigger_notification(notification_name):
    """Fire a named Pushcut notification by looking up its URL from AVAILABLE_NOTIFICATIONS."""
    noti_url = AVAILABLE_NOTIFICATIONS.get(notification_name)
    if noti_url:
        try:
            resp = requests.post(noti_url)
            logger.info(resp)
            return {}
        except Exception as e:
            logger.warning(f"Notification failed to fire. Error: {e}")

def send_notification(title: str = "Title", body: str = "Body", time_sensitive: bool = True, use_prefix: bool = True, prefix: str = None):
    """Send a Pushcut push notification via the custom webhook.

    :param title: notification title (prefixed with 'TravelNet — ' unless use_prefix=False).
    :param body: notification body text.
    :param time_sensitive: if True, uses the time-sensitive webhook (breaks through Focus).
    :param use_prefix: prepend 'TravelNet — ' to the title when True.
    """
    url = settings.custom_notification_time_sensitive if time_sensitive else settings.custom_notification_not_time_sensitive
    return _trigger_notification(url, title, body, use_prefix, prefix)

def journal_notification(title: str = "Title", body: str = "Body", time_sensitive: bool = False, use_prefix: bool = True, prefix: str = None):
    url = settings.journal_notification
    return _trigger_notification(url, title, body, use_prefix, prefix)

def label_known_place_notification(title: str = "Title", body: str = "Body", time_sensitive: bool = True, use_prefix: bool = True, prefix: str = None):
    url = settings.label_known_place_notification
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
    try:
        resp = requests.post(url, json=payload, headers=headers)
        return resp.json()
    except Exception as e:
        logger.warning(f"Notification failed to fire. Error: {e}")

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
    try: 
        resp = requests.post(url, json=payload, headers=headers)
        return resp.json()
    except Exception as e:
        logger.warning(f"Notification failed to fire. Error: {e}")

# ---------------------------------------------------------------------------
# Primitive
# ---------------------------------------------------------------------------

def send_email(
    subject: str,
    body: str,
    smtp_host: str,
    smtp_port: int,
    username: str,
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
        smtp.login(username, password)
        smtp.send_message(msg)

def _flush_and_send(smtp_cfg: dict) -> bool:
    """Read all pending cron_results, format a digest email, send it.
 
    Returns True on success.  On send failure, restores the records so the
    safety-net cron can retry.
    """
    records = daily_cron_table.fetch_and_clear()
    if not records:
        return True
 
    date = records[0].date  # all records share the same local date
    all_ok = all(r.success for r in records)
    status_icon = "✅" if all_ok else "❌"
 
    reported_names = {r.job_name for r in records}
    missing = [j for j in DAILY_CRON_JOBS if j not in reported_names]
 
    # ---- subject ----
    subject = f"[TravelNet] {status_icon} Daily cron digest — {date}"
 
    # ---- body ----
    lines = [f"Daily cron digest for {date}\n"]
 
    for r in records:
        icon = "✅" if r.success else "❌"
        dur = f"{r.duration_s:.1f}s" if r.duration_s is not None else "—"
        lines.append(f"{icon}  {r.job_name}  ({dur})  ran at {r.ran_at}")
 
        if r.metrics:
            try:
                metrics = json.loads(r.metrics)
                for k, v in metrics.items():
                    lines.append(f"      {k}: {v}")
            except (json.JSONDecodeError, AttributeError):
                lines.append(f"      metrics: {r.metrics}")
 
        if r.error:
            lines.append(f"      ERROR:\n{r.error}")
 
        lines.append("")
 
    if missing:
        lines.append("⚠️  Jobs that did not report:")
        for name in missing:
            lines.append(f"   • {name}")
 
    body = "\n".join(lines)
 
    try:
        send_email(subject=subject, body=body, **smtp_cfg)
        return True
    except Exception as mail_err:
        print(f"[DailyCronJobMailer] Failed to send digest: {mail_err}")
        # Put the records back so the safety-net cron can retry.
        try:
            daily_cron_table.restore(records)
        except Exception as restore_err:
            print(f"[DailyCronJobMailer] Failed to restore records: {restore_err}")
        return False
    
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

class DailyCronJobMailer(CronJobMailer):
    """Like CronJobMailer but writes to cron_results instead of sending an email.
 
    After writing, checks whether all DAILY_CRON_JOBS have reported for today.
    If so, flushes immediately and sends the digest.  If not, does nothing —
    the safety-net cron (send_cron_digest.py) will flush at a fixed time.
 
    Usage is identical to CronJobMailer::
 
        with DailyCronJobMailer("geocode_places", settings.smtp_config) as job:
            result = run()
            job.add_metric("geocoded", result["count"])
    """
 
    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        finished_at = datetime.now(tz=timezone.utc)
        duration_s = (finished_at - self._started_at).total_seconds()
 
        # Local date: use the timezone from timezone_transitions if available,
        # otherwise fall back to UTC date.  UTC is fine for the buffer key —
        # it just needs to be consistent within a single day's run.
        local_date = finished_at.strftime("%Y-%m-%d")
 
        metrics_json = json.dumps(dict(self._metrics)) if self._metrics else None
        error_text = traceback.format_exc() if exc_type is not None else None
 
        record = DailyCronRecord(
            job_name=self.job_name,
            ran_at=finished_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
            date=local_date,
            success=exc_type is None,
            duration_s=round(duration_s, 2),
            metrics=metrics_json,
            error=error_text,
        )
 
        try:
            daily_cron_table.insert(record)
        except Exception as db_err:
            print(f"[DailyCronJobMailer] Failed to record result for {self.job_name!r}: {db_err}")
 
        # Check whether all expected daily jobs have now reported for today.
        try:
            with get_conn() as conn:
                reported = {
                    row[0] for row in conn.execute(
                        "SELECT job_name FROM cron_results WHERE date = ?",
                        (local_date,),
                    ).fetchall()
                }
            if set(DAILY_CRON_JOBS).issubset(reported):
                _flush_and_send(self.smtp_cfg)
        except Exception as flush_err:
            print(f"[DailyCronJobMailer] Flush check failed: {flush_err}")
 
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
