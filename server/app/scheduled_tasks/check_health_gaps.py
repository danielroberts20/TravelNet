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
    on the weekly digest day). Silent otherwise.
  - Then hands confirmed gaps to the recovery flow (scheduled_tasks/recover_health_gaps.py):
    a Pushcut notification asks you to start HAE's server on the phone, and the gaps are
    pulled from it and verified straight away. That notification replaces the old critical
    push. If recovery is not configured the old behaviour applies: critical Pushcut, and
    a manual Health Auto Export checked off via POST /metadata/health-gaps/<id>/exported.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from prefect import flow, task, get_run_logger
from config.editable import load_overrides

load_overrides()

from config.general import (
    HEALTH_UPLOAD_STALE_HOURS,
    HEALTH_STALE_ALERT_FILE,
)
from database.connection import get_conn
from health_gaps.scan import recovery_configured, scan_and_notify
from notifications import error_notification, notify_on_completion, log_on_success, record_flow_result
from scheduled_tasks.recover_health_gaps import recover_health_gaps_flow


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
    return scan_and_notify(get_run_logger())


@task(name="recover_health_gaps")
def recover_gaps_from_phone() -> dict:
    """Ask the user to start HAE's server and pull confirmed gaps from the phone.

    Never raises: a recovery problem must not fail the daily check (the recovery flow
    sends its own failure notification).
    """
    log = get_run_logger()
    if not recovery_configured():
        return {"status": "disabled"}
    try:
        return recover_health_gaps_flow()
    except Exception as e:
        log.warning("Health gap recovery failed: %s", e)
        return {"status": "error"}


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(name="Check Health Gaps", on_failure=[notify_on_completion], on_completion=[log_on_success])
def check_health_gaps_flow() -> dict:
    """Daily health monitor: upload recency, then persistent gap detection."""
    recency = check_health_upload_recency()
    gaps = detect_and_notify_health_gaps()
    recovery = recover_gaps_from_phone()

    result = {"upload_stale": recency["stale"], "stale_hours": recency["stale_hours"], **gaps,
              "recovery": recovery.get("status")}
    record_flow_result(result)
    return result
