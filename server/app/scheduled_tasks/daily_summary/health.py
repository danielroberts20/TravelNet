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

import math
from datetime import datetime, timedelta, timezone as dt_timezone
from zoneinfo import ZoneInfo

from prefect import flow
from prefect.logging import get_run_logger

from notifications import record_flow_result
from database.connection import to_iso_str
from scheduled_tasks.daily_summary.base import Domain, closed_after
from config.general import THRESHOLD_HR_BPM


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
    # ML columns
    tss_data = _workout_tss(conn, ctx)
    data.update(tss_data)
    data.update(_atl_ctl(conn, ctx, tss_data.get("workout_tss")))
    data.update(_sleep_midpoint(conn, ctx))
    data.update(_mood_classification(conn, ctx))
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
    sleep_ts, wake_ts = _find_primary_sleep_session(conn, ctx)
    if not sleep_ts or not wake_ts:
        return {
            "wake_time_local": None, "sleep_time_local": None,
            "sleep_hours": None, "awake_hours": None,
            "deep_sleep_hours": None, "rem_sleep_hours": None,
            "light_sleep_hours": None, "sleep_efficiency_pct": None,
            "restorative_sleep_pct": None,
        }

    # Aggregate stages within the primary session window only
    rows = conn.execute("""
        SELECT stage, SUM(duration_hr) AS hours
        FROM health_sleep
        WHERE start_ts >= ? AND end_ts <= ?
        GROUP BY stage
    """, (sleep_ts, wake_ts)).fetchall()

    stages = {r["stage"]: r["hours"] for r in rows}
    deep  = stages.get("Deep",  0.0)
    rem   = stages.get("REM",   0.0)
    light = stages.get("Core",  0.0)
    awake = stages.get("Awake", 0.0)
    total = deep + rem + light

    efficiency  = round(total / (total + awake) * 100, 2) if (total + awake) > 0 else None
    restorative = round((deep + rem) / total * 100, 2) if total > 0 else None

    tz = ZoneInfo(ctx["timezone"])
    def to_local(ts):
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return to_iso_str(dt.astimezone(tz))

    return {
        "wake_time_local":       to_local(wake_ts),
        "sleep_time_local":      to_local(sleep_ts),
        "sleep_hours":           round(total, 2) if total else None,
        "awake_hours":           round(awake, 2) if awake else None,
        "deep_sleep_hours":      round(deep,  2) if deep  else None,
        "rem_sleep_hours":       round(rem,   2) if rem   else None,
        "light_sleep_hours":     round(light, 2) if light else None,
        "sleep_efficiency_pct":  efficiency,
        "restorative_sleep_pct": restorative,
    }


def _sleep_midpoint(conn, ctx: dict) -> dict:
    sleep_ts, wake_ts = _find_primary_sleep_session(conn, ctx)
    if not sleep_ts or not wake_ts:
        return {"sleep_midpoint_hr": None}

    tz = ZoneInfo(ctx["timezone"])
    local_date = datetime.strptime(ctx["date"], "%Y-%m-%d")
    midnight = local_date.replace(hour=0, minute=0, second=0, tzinfo=tz)

    sleep_dt = datetime.fromisoformat(
        sleep_ts.replace("Z", "+00:00")
    ).astimezone(tz)
    wake_dt = datetime.fromisoformat(
        wake_ts.replace("Z", "+00:00")
    ).astimezone(tz)

    # Compute midpoint as the actual datetime halfway between onset and wake,
    # then express that as hours from local midnight.
    # This correctly handles onset before midnight (negative hours from midnight
    # would corrupt the average if computed independently).
    total_secs = (wake_dt - sleep_dt).total_seconds()
    midpoint_dt = sleep_dt + timedelta(seconds=total_secs / 2)
    midpoint_hr = (midpoint_dt - midnight).total_seconds() / 3600

    return {"sleep_midpoint_hr": round(midpoint_hr, 2)}


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


def _workout_tss(conn, ctx: dict) -> dict:
    """
    Training Stress Score for all workouts on this day.
    Uses intensity_met if available, falls back to HR-based TSS.
    HR-based: TSS = (duration_hr) * (avg_hr / THRESHOLD_HR_BPM)^2 * 100
    """
    rows = conn.execute("""
        SELECT duration_s, intensity_met, hr_avg
        FROM workouts
        WHERE start_ts >= ? AND start_ts < ?
    """, (ctx["utc_start"], ctx["utc_end"])).fetchall()

    if not rows:
        return {"workout_tss": None}

    total = 0.0
    for row in rows:
        dur_hr = (row["duration_s"] or 0) / 3600.0
        if row["intensity_met"] is not None and row["intensity_met"] > 0:
            tss = dur_hr * (row["intensity_met"] / 10.0) * 100
        elif row["hr_avg"] is not None and row["hr_avg"] > 0:
            tss = dur_hr * (row["hr_avg"] / THRESHOLD_HR_BPM) ** 2 * 100
        else:
            tss = 0.0
        total += tss

    return {"workout_tss": round(total, 2)}


def _atl_ctl(conn, ctx: dict, workout_tss: float | None) -> dict:
    """
    Exponentially weighted ATL (7-day) and CTL (42-day).
    Reads previous day's atl/ctl from daily_summary.
    Cold start: prev_atl = prev_ctl = 0 when no prior row exists.
    """
    prev_date = (datetime.strptime(ctx["date"], "%Y-%m-%d") - timedelta(days=1)
                 ).strftime("%Y-%m-%d")
    prev = conn.execute(
        "SELECT atl, ctl FROM daily_summary WHERE date = ?", (prev_date,)
    ).fetchone()
    prev_atl = prev["atl"] if prev and prev["atl"] is not None else 0.0
    prev_ctl = prev["ctl"] if prev and prev["ctl"] is not None else 0.0

    tss = workout_tss if workout_tss is not None else 0.0
    atl_k = 1 - math.exp(-1 / 7)
    ctl_k = 1 - math.exp(-1 / 42)
    new_atl = prev_atl + atl_k * (tss - prev_atl)
    new_ctl = prev_ctl + ctl_k * (tss - prev_ctl)
    tsb = new_ctl - new_atl

    return {
        "atl": round(new_atl, 3),
        "ctl": round(new_ctl, 3),
        "tsb": round(tsb, 3),
    }

def _find_primary_sleep_session(
    conn, ctx: dict,
    lookback_hours: int = 10,
    max_gap_mins: int = 90,
    max_awake_bridge_mins: int = 20,
    min_session_hrs: float = 1.0,
) -> tuple[str | None, str | None]:
    utc_start_dt = datetime.fromisoformat(
        ctx["utc_start"].replace("Z", "+00:00")
    )
    lookback_utc = (
        utc_start_dt - timedelta(hours=lookback_hours)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Collect all stages from lookback until END of day (not noon)
    # — session selection handles excluding the evening block
    rows = conn.execute("""
        SELECT start_ts, end_ts, stage, duration_hr
        FROM health_sleep
        WHERE end_ts > ? AND start_ts < ?
        ORDER BY start_ts ASC
    """, (lookback_utc, ctx["utc_end"])).fetchall()

    if not rows:
        return None, None

    # Group into sessions (same logic as before)
    sessions: list[list] = []
    current: list = [rows[0]]

    for i in range(1, len(rows)):
        prev, curr = rows[i - 1], rows[i]

        gap_mins = (
            datetime.fromisoformat(curr["start_ts"].replace("Z", "+00:00")) -
            datetime.fromisoformat(prev["end_ts"].replace("Z", "+00:00"))
        ).total_seconds() / 60

        # Short Awake — bridge it, stay in current session
        if (curr["stage"] == "Awake"
                and curr["duration_hr"] * 60 <= max_awake_bridge_mins):
            current.append(curr)
            continue

        # Long Awake row — treat as a session break regardless of gap
        # (the Awake row stays in the current session for efficiency accounting,
        # then a new session starts with the next row)
        if (curr["stage"] == "Awake"
                and curr["duration_hr"] * 60 > max_awake_bridge_mins):
            current.append(curr)
            sessions.append(current)
            current = []
            continue

        if gap_mins > max_gap_mins:
            sessions.append(current)
            current = [curr]
        else:
            current.append(curr)

    sessions.append(current)

    # Filter to sessions with enough real sleep
    valid = [
        s for s in sessions
        if sum(r["duration_hr"] for r in s if r["stage"] != "Awake")
        >= min_session_hrs
    ]
    if not valid:
        valid = sessions

    # Prefer sessions whose end falls within today's UTC window
    # (i.e. end_ts >= utc_start). These are genuine morning wake-ups.
    # Only fall back to pre-midnight sessions if nothing ends in-window.
    in_window = [s for s in valid if s[-1]["end_ts"] >= ctx["utc_start"]]
    candidates = in_window if in_window else valid
    primary = min(candidates, key=lambda s: s[-1]["end_ts"])

    # Safety check: if the earliest-ending session ends after 18:00 local,
    # something is wrong — return None rather than corrupt data
    tz = ZoneInfo(ctx["timezone"])
    wake_dt = datetime.fromisoformat(
        primary[-1]["end_ts"].replace("Z", "+00:00")
    ).astimezone(tz)
    local_date = datetime.strptime(ctx["date"], "%Y-%m-%d")
    local_18h = local_date.replace(hour=18, tzinfo=tz)

    if wake_dt > local_18h:
        return None, None

    return primary[0]["start_ts"], primary[-1]["end_ts"]


def _mood_classification(conn, ctx: dict) -> dict:
    """Most frequent valence classification for the day; tie-broken by higher mean |valence|."""
    row = conn.execute("""
        SELECT classification, COUNT(*) AS n, AVG(ABS(valence)) AS avg_abs_valence
        FROM state_of_mind
        WHERE start_ts >= ? AND end_ts < ?
        GROUP BY classification
        ORDER BY n DESC, avg_abs_valence DESC
        LIMIT 1
    """, (ctx["utc_start"], ctx["utc_end"])).fetchone()

    return {"mood_classification": row["classification"] if row else None}


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
        # Training load
        "workout_tss", "atl", "ctl", "tsb",
        # Sleep
        "wake_time_local", "sleep_time_local", "sleep_hours", "awake_hours",
        "deep_sleep_hours", "rem_sleep_hours", "light_sleep_hours",
        "sleep_efficiency_pct", "restorative_sleep_pct",
        "sleep_midpoint_hr",
        # Mood
        "avg_valence", "mood_entries", "mood_classification",
    }),
    completeness_flag="health_complete",
    compute_fn=compute_health_data,
    # HAE uploads every 4 hours; a 4-day buffer catches any stragglers.
    completeness_predicate=closed_after(4),
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