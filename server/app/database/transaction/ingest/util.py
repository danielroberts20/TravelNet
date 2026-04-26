"""
database/transaction/ingest/util.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Shared helpers used by both the Revolut and Wise ingest modules.
"""

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

def maybe_mark_internal(row: dict) -> dict:
    desc = (row.get('description') or '').lower()
    payee = (row.get('payee') or '').lower()
    if any(name in desc or name in payee for name in SELF_NAMES):
        row['is_internal'] = 1
    return row
