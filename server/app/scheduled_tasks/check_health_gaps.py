"""
Daily health data gap detection and upload recency check.
Scheduled: Every day at 09:00 (see config/schedules.py).

  - Checks whether any health data has landed in the DB in the last
    HEALTH_UPLOAD_STALE_HOURS hours. Fires a Pushcut ERROR notification
    on first detection and again every 24h if still stale.
  - Scans for gaps using per-metric absolute thresholds (health_gaps/config.py),
    groups correlated metrics into one outage event, and persists every event in
    health_gap_events. Detection runs daily because data lost in a gap is
    unrecoverable; only *notification* is gated on state changes.
  - Emails only when a gap is newly confirmed (or, for digest-only metrics,
    on the weekly digest day). Pushcut for critical gaps. Silent otherwise.
  - Confirmed gaps can be fixed by a manual Health Auto Export; check them off
    via POST /metadata/health-gaps/<id>/exported and the next run verifies.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from prefect import flow, task, get_run_logger
from config.editable import load_overrides

load_overrides()

from config.general import (
    HEALTH_GAP_DIGEST_WEEKDAY,
    HEALTH_GAP_ESCALATE_DAYS,
    HEALTH_GAP_EXPORT_PAD_HOURS,
    HEALTH_GAP_EXPORT_VERIFY_DELAY_HOURS,
    HEALTH_GAP_LOOKBACK_DAYS,
    HEALTH_GAP_MIN_NIGHTS,
    HEALTH_MIN_POINTS_AFTER,
    HEALTH_UPLOAD_STALE_HOURS,
    HEALTH_STALE_ALERT_FILE,
)
from config.settings import settings
from database.connection import get_conn
from database.health.gap_events.table import table as gap_events_table
from health_gaps.events import Params
from health_gaps.runner import compute_scan_start, load_health_data, run_check
from notifications import error_notification, notify_on_completion, log_on_success, record_flow_result, send_email, send_notification


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _to_dt(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


# ---------------------------------------------------------------------------
# Suppression helpers
# ---------------------------------------------------------------------------

def _load_stale_alert_state(path: Path) -> datetime | None:
    """Return the timestamp of the last stale alert, or None if never fired."""
    try:
        data = json.loads(path.read_text())
        return _to_dt(data["alerted_at"])
    except (FileNotFoundError, KeyError, ValueError):
        return None


def _save_stale_alert_state(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"alerted_at": _now_str()}))


def _clear_stale_alert_state(path: Path) -> None:
    path.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(name="check_health_upload_recency")
def check_health_upload_recency() -> dict:
    """Fire a Pushcut ERROR if no health data has landed recently.

    Suppressed for 24h after first alert to avoid repeated notifications
    for the same outage. Re-fires every 24h if still stale.
    """
    log = get_run_logger()
    now = datetime.now(timezone.utc)

    conn = get_conn(read_only=True)
    try:
        latest_quantity = conn.execute(
            "SELECT MAX(timestamp) FROM health_quantity"
        ).fetchone()[0]
        latest_hr = conn.execute(
            "SELECT MAX(timestamp) FROM health_heart_rate"
        ).fetchone()[0]
    finally:
        conn.close()

    candidates = [ts for ts in (latest_quantity, latest_hr) if ts]
    if not candidates:
        stale_hours = None
        stale = True
        log.warning("No health data in DB at all")
    else:
        latest = max(_to_dt(ts) for ts in candidates)
        stale_hours = round((now - latest).total_seconds() / 3600, 1)
        stale = stale_hours > HEALTH_UPLOAD_STALE_HOURS
        log.info("Most recent health data: %s (%.1fh ago)", latest.isoformat(), stale_hours)

    if stale:
        last_alerted = _load_stale_alert_state(HEALTH_STALE_ALERT_FILE)
        suppress = (
            last_alerted is not None
            and (now - last_alerted).total_seconds() < 86400
        )
        if not suppress:
            msg = (
                f"No health data received for {stale_hours:.1f}h — HAE may have stopped uploading."
                if stale_hours is not None
                else "No health data in DB at all."
            )
            error_notification(msg)
            _save_stale_alert_state(HEALTH_STALE_ALERT_FILE)
            log.warning("Stale upload alert fired: %s", msg)
        else:
            hours_since = round((now - last_alerted).total_seconds() / 3600, 1)
            log.info("Stale upload suppressed — last alert was %.1fh ago", hours_since)
    else:
        _clear_stale_alert_state(HEALTH_STALE_ALERT_FILE)

    return {"stale": stale, "stale_hours": stale_hours}


@task(name="detect_and_notify_health_gaps")
def detect_and_notify_health_gaps() -> dict:
    log = get_run_logger()
    now = datetime.now(timezone.utc)
    params = Params(
        min_points_after=HEALTH_MIN_POINTS_AFTER,
        escalate_days=HEALTH_GAP_ESCALATE_DAYS,
        export_verify_delay_h=HEALTH_GAP_EXPORT_VERIFY_DELAY_HOURS,
        export_pad_h=HEALTH_GAP_EXPORT_PAD_HOURS,
    )

    gap_events_table.init()   # idempotent; flow may run before the API restarts
    state_conn = get_conn()
    try:
        scan_start = compute_scan_start(state_conn, now, HEALTH_GAP_LOOKBACK_DAYS)
        read_conn = get_conn(read_only=True)
        try:
            data = load_health_data(read_conn, scan_start)
        finally:
            read_conn.close()

        def _email(subject: str, body: str) -> None:
            log.info("Gap report:\n%s", body)
            send_email(subject=subject, body=body, **settings.smtp_config)

        def _push(text: str) -> None:
            log.warning("Critical health gap: %s", text)
            send_notification(title="Health data gap", body=text, time_sensitive=True)

        report = run_check(
            state_conn, data, now, scan_start,
            send_email=_email, push=_push,
            digest_weekday=HEALTH_GAP_DIGEST_WEEKDAY,
            min_nights=HEALTH_GAP_MIN_NIGHTS,
            params=params,
        )
    finally:
        state_conn.close()

    result = {
        "newly_confirmed": len(report.newly_confirmed),
        "held_for_digest": len(report.held_for_digest),
        "awaiting_export": len(report.awaiting_export),
        "ongoing": len(report.ongoing),
    }
    log.info("Health gap check: %s", result)
    return result


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(name="Check Health Gaps", on_failure=[notify_on_completion], on_completion=[log_on_success])
def check_health_gaps_flow() -> dict:
    """Daily health monitor: upload recency, then persistent gap detection."""
    recency = check_health_upload_recency()
    gaps = detect_and_notify_health_gaps()

    result = {"upload_stale": recency["stale"], "stale_hours": recency["stale_hours"], **gaps}
    record_flow_result(result)
    return result
