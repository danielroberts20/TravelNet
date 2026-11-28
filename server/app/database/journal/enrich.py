"""
database/journal/enrich.py
~~~~~~~~~~~~~~~~~~~~~~~~~~
Best-effort context snapshot for a journal entry, built from data TravelNet
already holds. Every lookup is independent and failures degrade to "missing":
a capture must never be rejected because enrichment found nothing.

All functions take an open sqlite3 connection (row_factory=sqlite3.Row).
"""

from datetime import datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

# Ignore location fixes older than this when placing an entry.
MAX_LOCATION_AGE = timedelta(hours=6)
_TS = "%Y-%m-%dT%H:%M:%SZ"


def _fmt(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime(_TS)


def latest_timezone(conn) -> ZoneInfo:
    """Most recent known timezone from transition_timezone, defaulting to UTC."""
    row = conn.execute(
        "SELECT to_tz FROM transition_timezone ORDER BY transitioned_at DESC LIMIT 1"
    ).fetchone()
    try:
        return ZoneInfo(row[0]) if row and row[0] else ZoneInfo("UTC")
    except Exception:
        return ZoneInfo("UTC")


def nearest_location(conn, ts_utc: datetime) -> Optional[dict]:
    """Last location fix at or before ts_utc (within MAX_LOCATION_AGE), with its place."""
    row = conn.execute(
        "SELECT timestamp, latitude, longitude, place_id FROM location_unified "
        "WHERE timestamp <= ? AND timestamp >= ? ORDER BY timestamp DESC LIMIT 1",
        (_fmt(ts_utc), _fmt(ts_utc - MAX_LOCATION_AGE)),
    ).fetchone()
    if not row:
        return None
    loc = {"latitude": row["latitude"], "longitude": row["longitude"],
           "place_id": row["place_id"], "fix_ts": row["timestamp"]}
    if row["place_id"] is not None:
        p = conn.execute(
            "SELECT country_code, country, region, locality, city, timezone FROM places WHERE id = ?",
            (row["place_id"],),
        ).fetchone()
        if p:
            loc.update({"country_code": p["country_code"], "country": p["country"],
                        "region": p["region"], "town": p["locality"] or p["city"],
                        "timezone": p["timezone"]})
    return loc


def nearest_weather(conn, ts_utc: datetime, lat: float, lon: float) -> Optional[dict]:
    """Closest weather_hourly row within ~0.15° and 90 min of the entry."""
    row = conn.execute(
        "SELECT timestamp, temperature_c, weathercode FROM weather_hourly "
        "WHERE timestamp BETWEEN ? AND ? AND ABS(latitude - ?) < 0.15 AND ABS(longitude - ?) < 0.15 "
        "ORDER BY ABS(strftime('%s', timestamp) - ?) LIMIT 1",
        (_fmt(ts_utc - timedelta(minutes=90)), _fmt(ts_utc + timedelta(minutes=90)),
         lat, lon, int(ts_utc.timestamp())),
    ).fetchone()
    if not row:
        return None
    return {"temp_c": row["temperature_c"], "weathercode": row["weathercode"]}


def steps_so_far(conn, day_start_utc: datetime, ts_utc: datetime) -> Optional[int]:
    """Step Count since local midnight, preferring Watch rows over iPhone duplicates."""
    row = conn.execute("""
        WITH deduped AS (
            SELECT value, ROW_NUMBER() OVER (
                PARTITION BY timestamp
                ORDER BY CASE WHEN source LIKE '%Watch%' THEN 0
                              WHEN source LIKE '%iPhone%' THEN 1 ELSE 2 END
            ) AS rn
            FROM health_quantity
            WHERE metric = 'Step Count' AND timestamp >= ? AND timestamp <= ?
        )
        SELECT SUM(value) AS total, COUNT(*) AS n FROM deduped WHERE rn = 1
    """, (_fmt(day_start_utc), _fmt(ts_utc))).fetchone()
    return int(row["total"]) if row and row["n"] else None


def spend_so_far(conn, day_start_utc: datetime, ts_utc: datetime) -> Optional[float]:
    """GBP spent since local midnight (same filters as the daily summary)."""
    row = conn.execute("""
        SELECT COALESCE(SUM(ABS(amount_gbp)), 0) AS spend, COUNT(*) AS n
        FROM transactions
        WHERE timestamp >= ? AND timestamp <= ?
          AND is_internal = 0 AND is_interest = 0
          AND (state != 'FAILED' OR state IS NULL)
          AND amount_gbp IS NOT NULL AND amount_gbp < 0
    """, (_fmt(day_start_utc), _fmt(ts_utc))).fetchone()
    return round(row["spend"], 2) if row and row["n"] else None


def enrich(conn, ts_utc: datetime) -> dict:
    """Return {tz, local_date, utc_offset, location, weather, steps_so_far, spend_gbp_so_far}.

    tz / local_date / utc_offset are always present. Everything else is omitted
    when its lookup finds nothing or fails.
    """
    out: dict = {}
    loc = None
    try:
        loc = nearest_location(conn, ts_utc)
    except Exception:
        pass

    tz = latest_timezone(conn)
    if loc and loc.get("timezone"):
        try:
            tz = ZoneInfo(loc["timezone"])
        except Exception:
            pass

    local = ts_utc.astimezone(tz)
    day_start_utc = local.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc)
    out["tz"] = tz.key
    out["local_date"] = local.date().isoformat()
    out["utc_offset"] = local.strftime("%z")

    if loc:
        out["location"] = loc
        try:
            w = nearest_weather(conn, ts_utc, loc["latitude"], loc["longitude"])
            if w:
                out["weather"] = w
        except Exception:
            pass

    for key, fn in (("steps_so_far", steps_so_far), ("spend_gbp_so_far", spend_so_far)):
        try:
            v = fn(conn, day_start_utc, ts_utc)
            if v is not None:
                out[key] = v
        except Exception:
            pass
    return out
