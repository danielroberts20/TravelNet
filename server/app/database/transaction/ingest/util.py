"""
database/transaction/ingest/util.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Shared helpers used by both the Revolut and Wise ingest modules.
"""

from typing import Optional


def safe_float(value: str) -> Optional[float]:
    """Parse a string to float, returning None for blank or unparseable values."""
    try:
        return float(value) if value.strip() != "" else None
    except (ValueError, AttributeError):
        return None

def get_closest_lat_lon_by_timestamp(cursor, timestamp: str) -> tuple[Optional[float], Optional[float]]:
    lat_lon = cursor.execute("""
        SELECT lat, lon
        FROM location_unified
        WHERE timestamp <= ?
        AND timestamp >= datetime(?, '-15 minutes')
        ORDER BY timestamp DESC
        LIMIT 1;
    """, (timestamp, timestamp)).fetchone()

    if not lat_lon:
        lat, lon = None, None
    else:
        lat, lon = lat_lon["lat"], lat_lon["lon"]

    return lat, lon