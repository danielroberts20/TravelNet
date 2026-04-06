"""
unified.py
~~~~~~~~~~
Queries the location_unified view and deduplicates overlapping
Overland and Shortcuts points in memory.

Rules:
  - If a Shortcuts point has an Overland point within TIME_WINDOW seconds
    and DIST_THRESHOLD degrees (~1km), prefer Overland and drop Shortcuts.
  - If they are far apart (> DIST_THRESHOLD), keep both — genuine divergence.
"""

from __future__ import annotations
import math
from typing import Any

from config.general import LOCATION_TIME_WINDOW as TIME_WINDOW, LOCATION_DIST_THRESHOLD as DIST_THRESHOLD


def get_unified_location(
    conn,
    since: int,
    until: int,
) -> list[dict[str, Any]]:
    """Return deduplicated location points between two Unix timestamps.

    Parameters
    ----------
    conn:  SQLite connection (row_factory = sqlite3.Row)
    since: Unix timestamp (seconds), inclusive
    until: Unix timestamp (seconds), inclusive
    """
    rows = conn.execute("""
        SELECT timestamp, latitude, longitude, altitude, activity,
               battery, speed, device, accuracy, source
        FROM location_unified
        WHERE datetime(timestamp) >= datetime(:since, 'unixepoch')
          AND datetime(timestamp) <= datetime(:until, 'unixepoch')
        ORDER BY timestamp ASC
    """, {"since": since, "until": until}).fetchall()

    points = [dict(r) for r in rows]
    return _deduplicate(points)


def _ts_to_seconds(ts: str) -> float:
    """Convert ISO 8601 UTC string to float seconds since epoch."""
    from datetime import datetime, timezone
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        # fallback — strip sub-seconds
        dt = datetime.strptime(ts[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    return dt.timestamp()


def _dist(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Approximate distance in degrees (fast, good enough for ~1km threshold)."""
    return math.sqrt((lat1 - lat2) ** 2 + (lon1 - lon2) ** 2)


def _deduplicate(points: list[dict]) -> list[dict]:
    """Drop Shortcuts points that are within TIME_WINDOW and DIST_THRESHOLD
    of an Overland point. Keep both if locations differ significantly."""

    overland  = [p for p in points if p["source"] == "overland"]
    shortcuts = [p for p in points if p["source"] == "shortcuts"]

    # Build a list of Overland timestamps in seconds for fast comparison
    overland_ts = [_ts_to_seconds(p["timestamp"]) for p in overland]

    kept_shortcuts = []
    for pt in shortcuts:
        pt_ts = _ts_to_seconds(pt["timestamp"])

        # Find any Overland point within TIME_WINDOW seconds
        matched = False
        for i, ots in enumerate(overland_ts):
            if abs(pt_ts - ots) <= TIME_WINDOW:
                op = overland[i]
                # Same location — drop Shortcuts point
                if _dist(pt["latitude"], pt["longitude"], op["latitude"], op["longitude"]) <= DIST_THRESHOLD:
                    matched = True
                    break
                # Different location — keep both (genuine divergence)

        if not matched:
            kept_shortcuts.append(pt)

    # Merge and re-sort
    merged = overland + kept_shortcuts
    merged.sort(key=lambda p: p["timestamp"])
    return merged