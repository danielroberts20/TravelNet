"""
scheduled_tasks/daily_summary/compute.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Master daily_summary flow.

For each date that needs (re)computing, calls each daily-cadence subflow
(health, location, pi). Transaction and weather backfills run on their
own monthly schedules and are NOT called from here.

Runs daily at 08:00.
"""
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

@task
def get_dates_to_compute() -> list[str]:
    """
    Return dates to process on this run:
      - Always yesterday (primary target)
      - Any date in the last RECOMPUTE_WINDOW_DAYS where a daily-cadence
        completeness flag (health/location/pi) is still 0
      - Any local date with source data but no daily_summary row yet
    """
    today_local  = datetime.now(dt_timezone.utc).date()
    window_start = today_local - timedelta(days=RECOMPUTE_WINDOW_DAYS)

    dates = set()
    dates.add((today_local - timedelta(days=1)).isoformat())

    with get_conn(read_only=True) as conn:
        rows = conn.execute("""
            SELECT date FROM daily_summary
            WHERE date >= ?
              AND (health_complete = 0
                OR location_complete = 0
                OR pi_complete = 0)
        """, (window_start.isoformat(),)).fetchall()
        dates.update(r["date"] for r in rows)

        rows = conn.execute("""
            SELECT DISTINCT substr(timestamp, 1, 10) AS d
            FROM location_unified
            WHERE timestamp >= ?
        """, (window_start.isoformat() + "T00:00:00Z",)).fetchall()
        dates.update(r["d"] for r in rows)

    return sorted(dates)


# ---------------------------------------------------------------------------
# Master flow
# ---------------------------------------------------------------------------

@flow(
    name="Compute Daily Summary",
    on_completion=[notify_on_completion],
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