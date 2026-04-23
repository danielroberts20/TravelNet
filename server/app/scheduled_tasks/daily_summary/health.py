"""
scheduled_tasks/daily_summary/health.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Owns the health, sleep, and mood columns of daily_summary.

Can run as a subflow (called from the master compute_daily_summary_flow)
or ad-hoc on a specific date:
    cd server && docker exec travelnet python -c \\
        "from scheduled_tasks.daily_summary.health import compute_health_flow; \\
         compute_health_flow('2026-04-19')"
"""
from config.editable import load_overrides
load_overrides()

from prefect import flow
from prefect.logging import get_run_logger

from notifications import record_flow_result
from database.connection import to_iso_str
from scheduled_tasks.daily_summary.base import Domain, closed_after
from datetime import datetime
from zoneinfo import ZoneInfo


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Source preference for timestamp-level dedup in health_quantity.
# Apple Health syncs Step Count / Walking + Running Distance / Flights Climbed
# from Apple Watch to iPhone (and vice versa), causing duplicate rows at
# identical timestamps. We prefer the Watch when both sources report.
_SOURCE_DEDUP_ORDER_SQL = """
    CASE
        WHEN source LIKE '%Watch%'  THEN 0
        WHEN source LIKE '%iPhone%' THEN 1
        ELSE 2
    END
"""


# ---------------------------------------------------------------------------
# Compute function
# ---------------------------------------------------------------------------

def compute_health_data(conn, ctx: dict) -> dict:
    """Aggregate health_quantity + health_sleep + state_of_mind for one day."""
    data = {}
    data.update(_health_quantity(conn, ctx))
    data.update(_vo2_max(conn, ctx))
    data.update(_sleep(conn, ctx))
    data.update(_mood(conn, ctx))
    return data


def _health_quantity(conn, ctx: dict) -> dict:
    rows = conn.execute(f"""
        WITH deduped AS (
            SELECT metric, value,
                   ROW_NUMBER() OVER (
                       PARTITION BY metric, timestamp
                       ORDER BY {_SOURCE_DEDUP_ORDER_SQL}
                   ) AS rn
            FROM health_quantity
            WHERE timestamp >= ? AND timestamp < ?
        )
        SELECT metric,
               SUM(value) AS total,
               AVG(value) AS avg
        FROM deduped
        WHERE rn = 1
        GROUP BY metric
    """, (ctx["utc_start"], ctx["utc_end"])).fetchall()

    m = {r["metric"]: r for r in rows}

    def s(name):    return m[name]["total"] if name in m else None
    def a(name):    return m[name]["avg"]   if name in m else None
    def i(v):       return int(v) if v is not None else None
    def r2(v):      return round(v, 2) if v is not None else None
    def kj_kcal(v): return round(v / 4.184, 2) if v is not None else None

    return {
        "steps":                 i(s("Step Count")),
        "active_energy_kcal":    kj_kcal(s("Active Energy")),
        "resting_energy_kcal":   kj_kcal(s("Resting Energy")),
        "distance_km":           r2(s("Walking + Running Distance")),
        "flights_climbed":       i(s("Flights Climbed")),
        "time_in_daylight_min":  i(s("Time in Daylight")),
        "avg_walking_speed_kmh": r2(a("Walking Speed")),
        "resting_hr":            r2(a("Resting Heart Rate")),
        "avg_hrv_ms":            r2(a("Heart Rate Variability")),
        "avg_spo2_pct":          r2(a("Blood Oxygen Saturation")),
        "respiratory_rate":      r2(a("Respiratory Rate")),
        "wrist_temp_c":          r2(a("Apple Sleeping Wrist Temperature")),
    }


def _vo2_max(conn, ctx: dict) -> dict:
    """Most recent VO2 Max reading on or before utc_end (not time-windowed)."""
    row = conn.execute("""
        SELECT value FROM health_quantity
        WHERE metric = 'VO2 Max' AND timestamp <= ?
        ORDER BY timestamp DESC LIMIT 1
    """, (ctx["utc_end"],)).fetchone()
    return {"vo2_max": round(row["value"], 2) if row else None}


def _sleep(conn, ctx: dict) -> dict:
    """
    Compute both sleep efficiency and restorative quality.

    Efficiency: time asleep / time in bed  — the classic clinical measure.
                Typical healthy: 85-95%.
    Restorative: (deep + rem) / total asleep — a quality indicator.
                 Typical healthy: 30-45%.
    """
    rows = conn.execute("""
        SELECT stage, SUM(duration_hr) AS hours
        FROM health_sleep
        WHERE end_ts >= ? AND start_ts < ?
        GROUP BY stage
    """, (ctx["utc_start"], ctx["utc_end"])).fetchall()
    stages = {r["stage"]: r["hours"] for r in rows}

    deep  = stages.get("Deep",  0.0)
    rem   = stages.get("REM",   0.0)
    light = stages.get("Core",  0.0)
    awake = stages.get("Awake", 0.0)
    total = deep + rem + light

    efficiency  = None
    restorative = None
    if (total + awake) > 0:
        efficiency = round(total / (total + awake) * 100, 2)
    if total > 0:
        restorative = round((deep + rem) / total * 100, 2)

    rows = conn.execute("""
        SELECT MIN(start_ts) AS sleep_time, MAX(end_ts) AS wake_time
        FROM health_sleep
        WHERE end_ts >= ? AND end_ts < ?
    """, (ctx["utc_start"], ctx["utc_end"])).fetchone()
    wake_time = rows["wake_time"]
    sleep_time = rows["sleep_time"]

    if wake_time:
        utc_dt = datetime.fromisoformat(wake_time.replace("Z", "+00:00"))
        wake_time = to_iso_str(utc_dt.astimezone(ZoneInfo(ctx["timezone"])))
    if sleep_time:
        utc_dt = datetime.fromisoformat(sleep_time.replace("Z", "+00:00"))
        sleep_time = to_iso_str(utc_dt.astimezone(ZoneInfo(ctx["timezone"])))

    return {
        "wake_time_local":      wake_time if wake_time else None,
        "sleep_time_local":     sleep_time if sleep_time else None,
        "sleep_hours":          round(total, 2) if total else None,
        "awake_hours":          round(awake, 2) if awake else None,
        "deep_sleep_hours":     round(deep,  2) if deep  else None,
        "rem_sleep_hours":      round(rem,   2) if rem   else None,
        "light_sleep_hours":    round(light, 2) if light else None,
        "sleep_efficiency_pct": efficiency,
        "restorative_sleep_pct": restorative,
    }


def _mood(conn, ctx: dict) -> dict:
    row = conn.execute("""
        SELECT AVG(valence) AS avg, COUNT(*) AS total
        FROM state_of_mind
        WHERE start_ts >= ? AND end_ts < ?
    """, (ctx["utc_start"], ctx["utc_end"])).fetchone()
    return {
        "avg_valence":  round(row["avg"], 3) if row["avg"] is not None else None,
        "mood_entries": row["total"] or 0,
    }


# ---------------------------------------------------------------------------
# Domain spec
# ---------------------------------------------------------------------------

HEALTH_DOMAIN = Domain(
    name="health",
    columns=frozenset({
        # Activity
        "steps", "active_energy_kcal", "resting_energy_kcal",
        "distance_km", "flights_climbed", "time_in_daylight_min",
        "avg_walking_speed_kmh", "vo2_max",
        # Vitals
        "resting_hr", "avg_hrv_ms", "avg_spo2_pct",
        "respiratory_rate", "wrist_temp_c",
        # Sleep
        "wake_time_local", "sleep_time_local", "sleep_hours", "awake_hours", 
        "deep_sleep_hours", "rem_sleep_hours", "light_sleep_hours",
        "sleep_efficiency_pct", "restorative_sleep_pct",
        # Mood
        "avg_valence", "mood_entries",
    }),
    completeness_flag="health_complete",
    compute_fn=compute_health_data,
    # HAE uploads every 4 hours; a 2-day buffer catches any stragglers.
    completeness_predicate=closed_after(2),
)


# ---------------------------------------------------------------------------
# Flow — callable standalone or as subflow of compute_daily_summary
# ---------------------------------------------------------------------------

@flow(
    name="Compute Daily Summary — Health"
)
def compute_health_flow(local_date: str) -> dict:
    logger = get_run_logger()
    data = HEALTH_DOMAIN.upsert_for_date(local_date)
    logger.info(f"{local_date}: health domain upserted "
                f"(steps={data.get('steps')}, sleep={data.get('sleep_hours')})")
    result = {"local_date": local_date, **data}
    record_flow_result(result)
    return result