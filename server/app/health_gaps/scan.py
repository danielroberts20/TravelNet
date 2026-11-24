"""
health_gaps/scan.py
~~~~~~~~~~~~~~~~~~~
One detect -> reconcile -> notify pass over the health data. Shared by the daily
check flow and by the recovery flow's post-pull rescan, so both judge gaps identically.
"""

from __future__ import annotations

from datetime import datetime, timezone

from config.general import (
    HEALTH_GAP_DIGEST_WEEKDAY,
    HEALTH_GAP_ESCALATE_DAYS,
    HEALTH_GAP_EXPORT_PAD_HOURS,
    HEALTH_GAP_EXPORT_VERIFY_DELAY_HOURS,
    HEALTH_GAP_LOOKBACK_DAYS,
    HEALTH_GAP_MIN_NIGHTS,
    HEALTH_MIN_POINTS_AFTER,
)
from config.settings import settings
from database.connection import get_conn
from database.health.gap_events.table import table as gap_events_table
from health_gaps.events import Params
from health_gaps.runner import compute_scan_start, load_health_data, run_check
from notifications import send_email, send_notification


def recovery_configured() -> bool:
    """Automatic recovery needs the phone's address, the token and the notification."""
    return bool(settings.hae_host and settings.hae_token and settings.health_gap_notification)


def scan_and_notify(log, now: datetime | None = None) -> dict:
    """Run one gap scan as of ``now``. ``log`` is a logging.Logger-like object."""
    now = now or datetime.now(timezone.utc)
    auto = recovery_configured()
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
            if auto:
                # The recovery flow's notification replaces the old critical push.
                log.info("Critical health gap (recovery notification will follow): %s", text)
                return
            log.warning("Critical health gap: %s", text)
            send_notification(title="Health data gap", body=text, time_sensitive=True)

        report = run_check(
            state_conn, data, now, scan_start,
            send_email=_email, push=_push,
            digest_weekday=HEALTH_GAP_DIGEST_WEEKDAY,
            min_nights=HEALTH_GAP_MIN_NIGHTS,
            params=params, auto_recovery=auto,
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
