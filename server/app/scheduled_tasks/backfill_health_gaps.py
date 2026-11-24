"""
Scan the whole health history for gaps (manual deployment 'backfill-health-gaps').

The daily check only looks back a week. This runs the same detector over everything,
silently: no per-event emails or pushes, just one summary email and a CSV of the events.

DRY BY DEFAULT: with write=False the scan reconciles into an in-memory copy of the events
table and nothing is saved. Run it dry, review the summary and the CSV, then run it again
with write=True to record the events. Recovering what it finds is the normal recovery flow
(recover-health-gaps), run in budgeted sessions.
"""

from __future__ import annotations

from datetime import datetime, timezone

from prefect import flow, get_run_logger
from config.editable import load_overrides

load_overrides()

from config.general import (
    DATA_DIR,
    HEALTH_GAP_ESCALATE_DAYS,
    HEALTH_GAP_EXPORT_PAD_HOURS,
    HEALTH_GAP_EXPORT_VERIFY_DELAY_HOURS,
    HEALTH_GAP_MIN_NIGHTS,
    HEALTH_GAP_PULL_PAD_HOURS,
    HEALTH_GAP_RECOVERY_MAX_REQUESTS,
    HEALTH_MIN_POINTS_AFTER,
)
from config.settings import settings
from database.connection import get_conn
from database.health.gap_events.table import table as gap_events_table
from health_gaps.backfill import build_summary, copy_events, events_csv, run_backfill_scan
from health_gaps.detect import parse
from health_gaps.events import Params
from health_gaps.runner import load_health_data
from notifications import log_on_success, notify_on_completion, record_flow_result, send_email


def _first_health_timestamp(conn) -> datetime:
    row = conn.execute(
        "SELECT MIN(t) FROM (SELECT MIN(timestamp) t FROM health_quantity "
        "UNION ALL SELECT MIN(timestamp) FROM health_heart_rate "
        "UNION ALL SELECT MIN(start_ts) FROM health_sleep)"
    ).fetchone()
    if not row or not row[0]:
        raise RuntimeError("No health data in the database; nothing to scan.")
    return parse(row[0])


@flow(name="Backfill Health Gaps", on_failure=[notify_on_completion], on_completion=[log_on_success])
def backfill_health_gaps_flow(write: bool = False, scan_from: str = "") -> dict:
    """Scan the whole health history for gaps. ``write=False`` (default) is a dry run.

    scan_from  ISO date/time to start from; default is the first health row in the database.
    """
    log = get_run_logger()
    now = datetime.now(timezone.utc)
    params = Params(
        min_points_after=HEALTH_MIN_POINTS_AFTER,
        escalate_days=HEALTH_GAP_ESCALATE_DAYS,
        export_verify_delay_h=HEALTH_GAP_EXPORT_VERIFY_DELAY_HOURS,
        export_pad_h=HEALTH_GAP_EXPORT_PAD_HOURS,
    )

    if write:
        gap_events_table.init()   # idempotent; makes sure the pull columns exist
    read_conn = get_conn(read_only=True)
    state_conn = get_conn() if write else None
    try:
        start = parse(scan_from) if scan_from else _first_health_timestamp(read_conn)
        log.info("Scanning health history %s -> %s (%s)", start.date(), now.date(), "WRITE" if write else "dry run")
        data = load_health_data(read_conn, start)
        if state_conn is None:
            state_conn = copy_events(read_conn)
        result = run_backfill_scan(
            state_conn, data, now, start,
            min_nights=HEALTH_GAP_MIN_NIGHTS, params=params,
            pad_h=HEALTH_GAP_PULL_PAD_HOURS, persisted=write,
        )
    finally:
        read_conn.close()
        if state_conn is not None:
            state_conn.close()

    csv_path = DATA_DIR / f"health_gap_backfill_{now:%Y%m%d}_{'written' if write else 'dry'}.csv"
    csv_path.write_text(events_csv(result))
    subject, body = build_summary(result, HEALTH_GAP_RECOVERY_MAX_REQUESTS)
    send_email(subject=subject, body=f"{body}\n\nEvent list: {csv_path}", **settings.smtp_config)

    stats = result.tier_stats()
    out = {
        "written": write,
        "events": len(result.events),
        "new": len(result.new_event_ids),
        "tier1": stats[1][0], "tier2": stats[2][0], "tier3": stats[3][0],
        "requests": result.total_requests,
        "csv": str(csv_path),
    }
    log.info("Backfill scan: %s", out)
    record_flow_result({k: v for k, v in out.items() if k != "csv"})
    return out
