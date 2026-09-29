"""
public/location.py
~~~~~~~~~~~~~~~~~~~
Query logic for the public location endpoint. Built for Constellation
(a separate app) to compute a "how far apart are we right now" stat, and
eventually a cumulative "distance traveled apart" stat from history.

Every coordinate that leaves this module is fuzzed with random jitter
before being returned — nothing here ever exposes a raw location_unified
row. Jitter is a random distance (up to PUBLIC_LOCATION_FUZZ_RADIUS_KM) in
a random bearing, not a fixed offset, so repeated calls can't be averaged
to recover the real point.
"""

import logging
import math
import random
import sqlite3
from datetime import datetime, timezone
from typing import Optional

from config.general import (
    LOCATION_CHANGE_RADIUS_M,
    PUBLIC_LOCATION_FUZZ_RADIUS_KM,
    PUBLIC_LOCATION_HISTORY_DOWNSAMPLE_MINUTES,
)
from database.connection import get_conn
from util import haversine_m

logger = logging.getLogger(__name__)

_EARTH_RADIUS_KM = 6371.0


# ---------------------------------------------------------------------------
# Fuzzing
# ---------------------------------------------------------------------------

def _fuzz(latitude: float, longitude: float) -> tuple[float, float]:
    """Jitter a coordinate by a random distance/bearing, up to PUBLIC_LOCATION_FUZZ_RADIUS_KM.

    Distance is drawn from [0.4, 1.0] * radius (never near-zero) so the
    fuzz always provides a meaningful privacy margin.
    """
    radius_km = PUBLIC_LOCATION_FUZZ_RADIUS_KM
    distance_km = random.uniform(radius_km * 0.4, radius_km)
    bearing = random.uniform(0, 2 * math.pi)
    ang_dist = distance_km / _EARTH_RADIUS_KM

    lat_rad = math.radians(latitude)
    lon_rad = math.radians(longitude)

    new_lat_rad = math.asin(
        math.sin(lat_rad) * math.cos(ang_dist)
        + math.cos(lat_rad) * math.sin(ang_dist) * math.cos(bearing)
    )
    new_lon_rad = lon_rad + math.atan2(
        math.sin(bearing) * math.sin(ang_dist) * math.cos(lat_rad),
        math.cos(ang_dist) - math.sin(lat_rad) * math.sin(new_lat_rad),
    )

    new_lat = math.degrees(new_lat_rad)
    new_lon = (math.degrees(new_lon_rad) + 540) % 360 - 180  # normalise to [-180, 180]

    return round(new_lat, 5), round(new_lon, 5)


# ---------------------------------------------------------------------------
# Timestamp helpers
# ---------------------------------------------------------------------------

def _parse_ts(ts: str) -> datetime:
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return datetime.strptime(ts[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)


def _format_ts(ts: str) -> Optional[str]:
    try:
        return _parse_ts(ts).strftime("%Y-%m-%dT%H:%M:%SZ")
    except (ValueError, AttributeError):
        return None


# ---------------------------------------------------------------------------
# Downsampling — reuses the existing "meaningful movement" threshold
# (LOCATION_CHANGE_RADIUS_M) rather than inventing a new one.
# ---------------------------------------------------------------------------

def _downsample(points: list[dict]) -> list[dict]:
    """Keep a point if enough time has passed since the last kept point, or
    if it represents a meaningful move (>= LOCATION_CHANGE_RADIUS_M). Always
    keeps the first and last point so the range's endpoints aren't lost."""
    if not points:
        return []

    kept = [points[0]]
    last = points[0]

    for point in points[1:]:
        elapsed_min = (_parse_ts(point["timestamp"]) - _parse_ts(last["timestamp"])).total_seconds() / 60
        moved_m = haversine_m(
            last["latitude"], last["longitude"],
            point["latitude"], point["longitude"],
        )
        if elapsed_min >= PUBLIC_LOCATION_HISTORY_DOWNSAMPLE_MINUTES or moved_m >= LOCATION_CHANGE_RADIUS_M:
            kept.append(point)
            last = point

    if kept[-1] is not points[-1]:
        kept.append(points[-1])

    return kept


# ---------------------------------------------------------------------------
# DB queries — always returns fuzzed coordinates
# ---------------------------------------------------------------------------

def get_latest_location() -> Optional[dict]:
    """Return the most recent fuzzed {latitude, longitude, last_synced}, or None."""
    try:
        with get_conn(read_only=True) as conn:
            row = conn.execute("""
                SELECT timestamp, latitude, longitude
                FROM location_unified
                ORDER BY timestamp DESC
                LIMIT 1
            """).fetchone()
    except (sqlite3.Error, OSError) as e:
        logger.error(f"Failed to query latest public location: {e}")
        return None

    if row is None:
        return None

    latitude, longitude = _fuzz(row["latitude"], row["longitude"])
    return {
        "latitude": latitude,
        "longitude": longitude,
        "last_synced": _format_ts(row["timestamp"]),
    }


def get_location_history(since: datetime) -> list[dict]:
    """Return a time-ordered, downsampled, fuzzed history from `since` forward."""
    if since.tzinfo is None:
        since = since.replace(tzinfo=timezone.utc)
    since_str = since.strftime("%Y-%m-%dT%H:%M:%SZ")

    try:
        with get_conn(read_only=True) as conn:
            rows = conn.execute("""
                SELECT timestamp, latitude, longitude
                FROM location_unified
                WHERE timestamp >= ?
                ORDER BY timestamp ASC
            """, (since_str,)).fetchall()
    except (sqlite3.Error, OSError) as e:
        logger.error(f"Failed to query public location history: {e}")
        return []

    points = _downsample([dict(r) for r in rows])

    result = []
    for point in points:
        latitude, longitude = _fuzz(point["latitude"], point["longitude"])
        result.append({
            "latitude": latitude,
            "longitude": longitude,
            "timestamp": _format_ts(point["timestamp"]),
        })
    return result
