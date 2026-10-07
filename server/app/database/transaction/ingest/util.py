"""
database/transaction/ingest/util.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Shared helpers used by both the Revolut and Wise ingest modules.
"""

from datetime import datetime, timedelta
from typing import Optional
from config.general import SELF_NAMES


def safe_float(value: str) -> Optional[float]:
    """Parse a string to float, returning None for blank or unparseable values."""
    try:
        return float(value) if value.strip() != "" else None
    except (ValueError, AttributeError):
        return None

def get_closest_lat_lon_by_timestamp(cursor, timestamp: str) -> tuple[Optional[float], Optional[float]]:
    lat_lon = cursor.execute("""
        SELECT latitude, longitude
        FROM location_unified
        WHERE timestamp <= ?
        AND timestamp >= datetime(?, '-15 minutes')
        ORDER BY timestamp DESC
        LIMIT 1;
    """, (timestamp, timestamp)).fetchone()

    if not lat_lon:
        lat, lon = None, None
    else:
        lat, lon = lat_lon["latitude"], lat_lon["longitude"]

    return lat, lon

def get_nearest_lat_lon_within_hours(cursor, timestamp: str, hours: float = 12) -> tuple[Optional[float], Optional[float]]:
    """Nearest location fix (before or after) within +/- `hours` of timestamp.

    For sources with no time of day (e.g. CommBank statements, which carry only a
    date), where get_closest_lat_lon_by_timestamp's 15-minute look-back would
    almost never match.

    Compares the raw timestamp column against precomputed bounds (one indexed
    lookup each side) — wrapping the column in julianday() defeats the index and
    scans the whole location_unified view per call.
    """
    target = datetime.fromisoformat(timestamp.replace("Z", "+00:00")).replace(tzinfo=None)
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    target_s = target.strftime(fmt)
    window = timedelta(hours=hours)

    before = cursor.execute("""
        SELECT timestamp, latitude, longitude FROM location_unified
        WHERE timestamp <= ? AND timestamp >= ?
        ORDER BY timestamp DESC LIMIT 1
    """, (target_s, (target - window).strftime(fmt))).fetchone()
    after = cursor.execute("""
        SELECT timestamp, latitude, longitude FROM location_unified
        WHERE timestamp > ? AND timestamp <= ?
        ORDER BY timestamp ASC LIMIT 1
    """, (target_s, (target + window).strftime(fmt))).fetchone()

    def gap(row):
        ts = datetime.fromisoformat(row["timestamp"].replace("Z", "+00:00")).replace(tzinfo=None)
        return abs(ts - target)

    candidates = [r for r in (before, after) if r]
    if not candidates:
        return None, None
    best = min(candidates, key=gap)
    return best["latitude"], best["longitude"]

def maybe_mark_internal(row: dict) -> dict:
    desc = (row.get('description') or '').lower()
    payee = (row.get('payee') or '').lower()
    if any(name in desc or name in payee for name in SELF_NAMES):
        row['is_internal'] = 1
    return row
