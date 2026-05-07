"""
scheduled_tasks/daily_summary/weather.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Owns the weather columns of daily_summary.

Weather fetch happens daily at 05:30. This summary flow runs daily at
06:00, processing only dates where weather_complete = 0 and weather
data is available in weather_hourly.
"""
from config.editable import load_overrides
load_overrides()

from prefect import flow, task
from prefect.logging import get_run_logger

from config.general import COORD_PRECISION
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
            "temp_max_c":                  None,
            "temp_min_c":                  None,
            "precipitation_mm":            None,
            "weathercode":                 None,
            "uv_index_max":                None,
            "temp_avg_c":                  None,
            "shortwave_radiation_avg_wm2": None,
            "relative_humidity_avg_pct":   None,
            "surface_pressure_avg_hpa":    None,
            "daylight_duration_s":         None,
            "sunshine_duration_s":         None,
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
    """Temp extremes and daily fields from weather_daily; aggregates from hourly."""
    dl = conn.execute("""
        SELECT precipitation_sum_mm, temp_max_c, temp_min_c,
               daylight_duration_s, sunshine_duration_s
        FROM weather_daily
        WHERE date = ? AND latitude = ? AND longitude = ?
    """, (ctx["date"], lat, lon)).fetchone()

    wcode_row = conn.execute("""
        SELECT weathercode, COUNT(*) AS n FROM weather_hourly
        WHERE timestamp >= ? AND timestamp < ?
          AND ROUND(latitude,  ?) = ? AND ROUND(longitude, ?) = ?
          AND weathercode IS NOT NULL
        GROUP BY weathercode ORDER BY n DESC LIMIT 1
    """, (ctx["utc_start"], ctx["utc_end"],
          COORD_PRECISION, lat, COORD_PRECISION, lon)).fetchone()

    agg_row = conn.execute("""
        SELECT MAX(uv_index)                AS uv_max,
               AVG(temperature_c)           AS temp_avg,
               AVG(shortwave_radiation_wm2) AS sw_avg,
               AVG(relative_humidity_pct)   AS rh_avg,
               AVG(surface_pressure_hpa)    AS sp_avg
        FROM weather_hourly
        WHERE timestamp >= ? AND timestamp < ?
          AND ROUND(latitude,  ?) = ? AND ROUND(longitude, ?) = ?
    """, (ctx["utc_start"], ctx["utc_end"],
          COORD_PRECISION, lat, COORD_PRECISION, lon)).fetchone()

    def _r(val):
        return round(val, 2) if val is not None else None

    return {
        "temp_max_c":                  _r(dl["temp_max_c"])    if dl else None,
        "temp_min_c":                  _r(dl["temp_min_c"])    if dl else None,
        "precipitation_mm":            dl["precipitation_sum_mm"] if dl else None,
        "weathercode":                 wcode_row["weathercode"] if wcode_row else None,
        "uv_index_max":                _r(agg_row["uv_max"])   if agg_row else None,
        "temp_avg_c":                  _r(agg_row["temp_avg"]) if agg_row else None,
        "shortwave_radiation_avg_wm2": _r(agg_row["sw_avg"])   if agg_row else None,
        "relative_humidity_avg_pct":   _r(agg_row["rh_avg"])   if agg_row else None,
        "surface_pressure_avg_hpa":    _r(agg_row["sp_avg"])   if agg_row else None,
        "daylight_duration_s":         dl["daylight_duration_s"]  if dl else None,
        "sunshine_duration_s":         dl["sunshine_duration_s"]  if dl else None,
    }


# ---------------------------------------------------------------------------
# Domain spec
# ---------------------------------------------------------------------------

WEATHER_DOMAIN = Domain(
    name="weather",
    columns=frozenset({
        "temp_max_c", "temp_min_c", "precipitation_mm", "weathercode", "uv_index_max",
        "temp_avg_c", "shortwave_radiation_avg_wm2", "relative_humidity_avg_pct",
        "surface_pressure_avg_hpa", "daylight_duration_s", "sunshine_duration_s",
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
    with get_conn(read_only=True) as conn:
        rows = conn.execute("""
            SELECT ds.date
            FROM daily_summary ds
            WHERE ds.weather_complete = 0
              AND EXISTS (
                  SELECT 1 FROM weather_hourly wh
                  WHERE DATE(wh.timestamp) = ds.date
              )
            ORDER BY ds.date ASC
        """).fetchall()
    return [r["date"] for r in rows]


@flow(
    name="Backfill Weather in Summary",
    on_completion=[notify_on_completion],
    on_failure=[notify_on_completion],
)
def backfill_weather_in_summary_flow():
    """
    Daily: compute weather columns for any date where weather_complete = 0
    and weather data exists. Marks weather_complete = 1 on success.
    Scheduled: daily at 06:00 (30 min after the weather fetch at 05:30).
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