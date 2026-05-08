"""
scheduled_tasks/daily_summary/compute.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Master daily_summary flow.

For each date that needs (re)computing, calls each daily-cadence subflow
(health, location, pi). Transaction and weather backfills run on their
own monthly schedules and are NOT called from here.

Runs daily at 08:00.
"""
from zoneinfo import ZoneInfo

from config.editable import load_overrides
load_overrides()

from datetime import datetime, timedelta, timezone as dt_timezone

from prefect import flow, task
from prefect.logging import get_run_logger

from database.connection import get_conn
from notifications import notify_on_completion, record_flow_result
from config.general import RECOMPUTE_WINDOW_DAYS

from scheduled_tasks.daily_summary.health   import compute_health_flow
from scheduled_tasks.daily_summary.location import compute_location_flow
from scheduled_tasks.daily_summary.pi       import compute_pi_flow


# ---------------------------------------------------------------------------
# Determine dates
# ---------------------------------------------------------------------------

def _get_current_timezone(conn) -> str:
    row = conn.execute("""
        SELECT to_tz FROM transition_timezone
        ORDER BY transitioned_at DESC
        LIMIT 1
    """).fetchone()
    return row["to_tz"] if row else "UTC"
    
@task
def get_dates_to_compute() -> list[str]:
    with get_conn(read_only=True) as conn:
        tz_name = _get_current_timezone(conn)
        tz = ZoneInfo(tz_name)
        yesterday_local = datetime.now(tz).date() - timedelta(days=1)
        window_start = yesterday_local - timedelta(days=RECOMPUTE_WINDOW_DAYS)

        # Compute utc_end for yesterday — local midnight tonight converted to UTC
        yesterday_midnight_local = datetime(
            yesterday_local.year, yesterday_local.month, yesterday_local.day,
            tzinfo=tz
        )
        yesterday_utc_end = (
            yesterday_midnight_local + timedelta(days=1)
        ).astimezone(dt_timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        dates = set()
        dates.add(yesterday_local.isoformat())

        rows = conn.execute("""
            SELECT date FROM daily_summary
            WHERE date >= ? AND date <= ?
              AND (health_complete = 0
                OR location_complete = 0
                OR pi_complete = 0)
        """, (window_start.isoformat(), yesterday_local.isoformat())).fetchall()
        dates.update(r["date"] for r in rows)

        rows = conn.execute("""
            SELECT DISTINCT substr(timestamp, 1, 10) AS d
            FROM location_unified
            WHERE timestamp >= ? AND timestamp < ?
        """, (
            window_start.isoformat() + "T00:00:00Z",
            yesterday_utc_end,
        )).fetchall()
        dates.update(r["d"] for r in rows)

    return sorted(dates)

# ---------------------------------------------------------------------------
# Master flow
# ---------------------------------------------------------------------------

@flow(
    name="Compute Daily Summary",
    on_failure=[notify_on_completion],
)
def compute_daily_summary_flow():
    logger = get_run_logger()
    dates  = get_dates_to_compute()
    logger.info(f"Processing daily_summary for {len(dates)} date(s): {dates}")

    # Location must run first — transactions and weather both read
    # daily_summary.country_code / .city / .dominant_place_id, so those
    # need to be populated before any later subflow relies on them.
    # Health and pi are independent of location data and can run in any order.
    order = [
        ("location", compute_location_flow),
        ("health",   compute_health_flow),
        ("pi",       compute_pi_flow),
    ]

    results = {name: {"ok": 0, "failed": 0} for name, _ in order}

    for date in dates:
        for name, subflow in order:
            try:
                subflow(date)
                results[name]["ok"] += 1
            except Exception as e:
                logger.error(f"{date}: {name} subflow failed: {e}")
                results[name]["failed"] += 1

    summary = {
        "dates_considered": len(dates),
        **{f"{name}_ok":     r["ok"]     for name, r in results.items()},
        **{f"{name}_failed": r["failed"] for name, r in results.items()},
    }
    record_flow_result(summary)
    return summary