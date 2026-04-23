"""
scheduled_tasks/daily_summary/weather.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Owns the weather columns of daily_summary.

Weather fetch happens monthly on the 14th for the previous ~40 days, so
this domain also runs monthly in a backfill mode. Explicitly sets
weather_complete = 1 after each pass (not based on calendar age).
"""
from config.editable import load_overrides
load_overrides()

from datetime import datetime, timedelta, timezone as dt_timezone

from prefect import flow, task
from prefect.logging import get_run_logger

from config.general import COORD_PRECISION, BACKFILL_DAYS
from database.connection import get_conn
from notifications import notify_on_completion, record_flow_result
from scheduled_tasks.daily_summary.base import Domain, never_auto_close





# ---------------------------------------------------------------------------
# Compute function
# ---------------------------------------------------------------------------

def compute_weather_data(conn, ctx: dict) -> dict:
    lat, lon = _dominant_coords(conn, ctx["date"])
    if lat is None or lon is None:
        # No dominant location means no weather — return all NULLs
        return {
            "temp_max_c":       None,
            "temp_min_c":       None,
            "precipitation_mm": None,
            "weathercode":      None,
        }
    return _weather(conn, ctx, lat, lon)


def _dominant_coords(conn, local_date: str) -> tuple:
    """Fetch lat/lon for the day's dominant place, rounded to COORD_PRECISION."""
    row = conn.execute("""
        SELECT p.lat_snap, p.lon_snap
        FROM daily_summary ds
        JOIN places p ON p.id = ds.dominant_place_id
        WHERE ds.date = ?
    """, (local_date,)).fetchone()
    if not row:
        return None, None
    return round(row["lat_snap"], COORD_PRECISION), round(row["lon_snap"], COORD_PRECISION)


def _weather(conn, ctx: dict, lat: float, lon: float) -> dict:
    """Temp extremes from hourly; precipitation from daily; wcode: most frequent hourly."""
    hr = conn.execute("""
        SELECT MAX(temperature_c) AS max_c, MIN(temperature_c) AS min_c
        FROM weather_hourly
        WHERE timestamp >= ? AND timestamp < ?
          AND latitude = ? AND longitude = ?
    """, (ctx["utc_start"], ctx["utc_end"], lat, lon)).fetchone()

    dl = conn.execute("""
        SELECT precipitation_sum_mm
        FROM weather_daily
        WHERE date = ? AND latitude = ? AND longitude = ?
    """, (ctx["date"], lat, lon)).fetchone()

    wcode_row = conn.execute("""
        SELECT weathercode, COUNT(*) AS n FROM weather_hourly
        WHERE timestamp >= ? AND timestamp < ?
          AND latitude = ? AND longitude = ?
          AND weathercode IS NOT NULL
        GROUP BY weathercode ORDER BY n DESC LIMIT 1
    """, (ctx["utc_start"], ctx["utc_end"], lat, lon)).fetchone()

    return {
        "temp_max_c":       round(hr["max_c"], 2) if hr and hr["max_c"] is not None else None,
        "temp_min_c":       round(hr["min_c"], 2) if hr and hr["min_c"] is not None else None,
        "precipitation_mm": dl["precipitation_sum_mm"] if dl else None,
        "weathercode":      wcode_row["weathercode"]   if wcode_row else None,
    }


# ---------------------------------------------------------------------------
# Domain spec
# ---------------------------------------------------------------------------

WEATHER_DOMAIN = Domain(
    name="weather",
    columns=frozenset({
        "temp_max_c", "temp_min_c", "precipitation_mm", "weathercode",
    }),
    completeness_flag="weather_complete",
    compute_fn=compute_weather_data,
    # Weather fetched monthly; flag set explicitly by backfill flow.
    completeness_predicate=never_auto_close,
)


# ---------------------------------------------------------------------------
# Flows
# ---------------------------------------------------------------------------

@flow(
    name="Compute Daily Summary — Weather",
    on_completion=[notify_on_completion],
    on_failure=[notify_on_completion],
)
def compute_weather_flow(local_date: str) -> dict:
    """Ad-hoc single-date weather upsert. Does NOT set weather_complete."""
    logger = get_run_logger()
    data = WEATHER_DOMAIN.upsert_for_date(local_date)
    logger.info(f"{local_date}: weather upserted "
                f"(max={data.get('temp_max_c')}, min={data.get('temp_min_c')})")
    result = {"local_date": local_date, **data}
    record_flow_result(result)
    return result


@task
def find_dates_in_window() -> list[str]:
    cutoff = (datetime.now(dt_timezone.utc).date() - timedelta(days=BACKFILL_DAYS))
    with get_conn(read_only=True) as conn:
        rows = conn.execute("""
            SELECT date FROM daily_summary WHERE date >= ? ORDER BY date ASC
        """, (cutoff.isoformat(),)).fetchall()
    return [r["date"] for r in rows]


@flow(
    name="Backfill Weather in Summary",
    on_completion=[notify_on_completion],
    on_failure=[notify_on_completion],
)
def backfill_weather_in_summary_flow():
    """
    Monthly: recompute weather columns for every date in the last
    BACKFILL_DAYS days and mark weather_complete = 1.
    Scheduled: 14th of each month at 04:30 (30 min after the weather fetch).
    """
    logger = get_run_logger()
    dates  = find_dates_in_window()
    logger.info(f"Backfilling weather for {len(dates)} dates")

    updated = 0
    failed  = 0
    for d in dates:
        try:
            WEATHER_DOMAIN.upsert_for_date(d)
            with get_conn() as conn:
                conn.execute(
                    "UPDATE daily_summary SET weather_complete = 1 WHERE date = ?",
                    (d,),
                )
            updated += 1
        except Exception as e:
            logger.error(f"Failed to backfill weather for {d}: {e}")
            failed += 1

    result = {"dates_considered": len(dates),
              "updated": updated, "failed": failed}
    record_flow_result(result)
    return result