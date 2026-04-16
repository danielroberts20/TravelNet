"""
scheduled_tasks/retroactive_location_scan.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Retroactively scans location history to detect stays that the real-time
trigger missed — most commonly because the device was offline (Overland
buffers points) or because no GPS points were generated while stationary.

Algorithm
---------
1. Read a marker file (DATA_DIR/retroactive_location_scan.marker) to find
   the last processed timestamp.  On first run the marker is absent and the
   scan starts from the earliest location point in the database.
2. Fetch all points from (marker - LOCATION_STAY_DURATION_MINS) onward in
   chronological order.  The overlap ensures streaks that span the previous
   cursor boundary are never split across runs.
3. Walk forward through the points, building contiguous stationary clusters
   anchored on each cluster's first point.  A cluster breaks when a point
   falls outside LOCATION_STATIONARITY_RADIUS_M of the anchor.
4. For each cluster that meets the minimum-points and duration thresholds:
   - If the centroid matches a known place and duration >= LOCATION_REVISIT_DURATION_MINS
     → record a return visit (idempotency check prevents duplicates).
   - If no known place matches and duration >= LOCATION_STAY_DURATION_MINS
     → create a new known place and first visit.
5. Run check_departure() to close any open visits whose departure is now
   confirmed by the historical data.
6. Write the timestamp of the last scanned point back to the marker file.

Idempotency
-----------
The scan is safe to run multiple times.  visit_exists() guards against
duplicate visit rows; get_nearest_known_place() always queries the live DB
so new places created mid-scan are immediately visible to subsequent
clusters in the same run.

Scheduling
----------
Registered as "retroactive-location-scan" in SCHEDULE_CONFIGS.
Runs every other night at 03:15 (offset from other nightly jobs).
"""
from config.editable import load_overrides
load_overrides()

import json
import logging
from datetime import datetime, timedelta, timezone

from prefect import task, flow
from prefect.logging import get_run_logger
from prefect.cache_policies import NO_CACHE

from config.general import (
    DATA_DIR,
    LOCATION_STATIONARITY_RADIUS_M,
    LOCATION_MINIMUM_POINTS,
    LOCATION_REVISIT_DURATION_MINS,
    LOCATION_STAY_DURATION_MINS,
    LOCATION_DEPARTURE_CONFIRMATION_MINS,
)
from database.connection import get_conn
from notifications import record_flow_result
from triggers.location_change import (
    check_departure,
    get_nearest_known_place,
    visit_exists,
    _handle_known_place,
    _handle_new_place,
    _streak_duration_mins,
)
from util import haversine_m

logger = logging.getLogger(__name__)

MARKER_PATH = DATA_DIR / "retroactive_location_scan.marker"


# ---------------------------------------------------------------------------
# Marker file helpers
# ---------------------------------------------------------------------------

def _read_marker() -> str | None:
    """Return the last-processed ISO timestamp from the marker file, or None."""
    try:
        with open(MARKER_PATH) as f:
            data = json.load(f)
        return data.get("last_processed")
    except (FileNotFoundError, json.JSONDecodeError, KeyError):
        return None


def _write_marker(timestamp: str) -> None:
    """Atomically update the marker file with the given ISO timestamp."""
    import os
    tmp = MARKER_PATH.with_suffix(".tmp")
    with open(tmp, "w") as f:
        json.dump({"last_processed": timestamp}, f)
    os.replace(tmp, MARKER_PATH)


# ---------------------------------------------------------------------------
# Point fetching
# ---------------------------------------------------------------------------

def _get_points_from(cursor: str | None) -> list:
    """Fetch location points at or after cursor (adjusted for overlap), chronological.

    The overlap of LOCATION_STAY_DURATION_MINS ensures that a stationary streak
    straddling the previous cursor boundary is captured in full on this run.
    The cutoff is computed in Python to stay in the same ISO 8601 format
    (YYYY-MM-DDTHH:MM:SSZ) that the location_unified table uses, avoiding
    mismatch with SQLite's datetime() format.
    """
    with get_conn(read_only=True) as conn:
        if cursor is None:
            rows = conn.execute("""
                SELECT timestamp, latitude, longitude
                FROM location_unified
                ORDER BY timestamp ASC
            """).fetchall()
        else:
            cutoff_dt = datetime.fromisoformat(cursor) - timedelta(minutes=LOCATION_STAY_DURATION_MINS)
            if cutoff_dt.tzinfo is None:
                cutoff_dt = cutoff_dt.replace(tzinfo=timezone.utc)
            cutoff = cutoff_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            rows = conn.execute("""
                SELECT timestamp, latitude, longitude
                FROM location_unified
                WHERE timestamp >= ?
                ORDER BY timestamp ASC
            """, (cutoff,)).fetchall()
    return rows


# ---------------------------------------------------------------------------
# Forward-scan cluster detection
# ---------------------------------------------------------------------------

def _scan_for_stays(points: list, logger) -> int:
    """Walk chronologically-ordered points, detect stationary clusters, record stays.

    Returns the number of stays processed (new or revisit).
    """
    n = len(points)
    processed = 0
    i = 0

    while i < n:
        anchor_lat = points[i]['latitude']
        anchor_lon = points[i]['longitude']
        streak_start = points[i]['timestamp']

        # Extend cluster while points remain within stationarity radius of anchor
        j = i
        while j < n and haversine_m(
            anchor_lat, anchor_lon,
            points[j]['latitude'], points[j]['longitude']
        ) <= LOCATION_STATIONARITY_RADIUS_M:
            j += 1

        cluster = points[i:j]

        if len(cluster) >= LOCATION_MINIMUM_POINTS:
            streak_end = cluster[-1]['timestamp']
            duration   = _streak_duration_mins(streak_start, streak_end)

            avg_lat = sum(p['latitude']  for p in cluster) / len(cluster)
            avg_lon = sum(p['longitude'] for p in cluster) / len(cluster)

            nearest = get_nearest_known_place(avg_lat, avg_lon)

            if nearest is not None:
                if duration >= LOCATION_REVISIT_DURATION_MINS:
                    if not visit_exists(nearest['id'], streak_start):
                        opened = _handle_known_place(nearest, streak_start)
                        if opened:
                            logger.info(
                                "Retroactive revisit: place %d at %s (%.1f min, %d pts)",
                                nearest['id'], streak_start, duration, len(cluster)
                            )
                            processed += 1
                        else:
                            logger.info(
                                "Retroactive: place %d still has open visit near %s — skipping",
                                nearest['id'], streak_start
                            )
                    else:
                        logger.info(
                            "Retroactive: visit already exists for place %d near %s — skipping",
                            nearest['id'], streak_start
                        )
            else:
                if duration >= LOCATION_STAY_DURATION_MINS:
                    # Re-query — a previous iteration in this run may have created
                    # a nearby place that now matches.
                    nearest_recheck = get_nearest_known_place(avg_lat, avg_lon)
                    if nearest_recheck is not None:
                        if not visit_exists(nearest_recheck['id'], streak_start):
                            opened = _handle_known_place(nearest_recheck, streak_start)
                            if opened:
                                logger.info(
                                    "Retroactive revisit (recheck): place %d at %s",
                                    nearest_recheck['id'], streak_start
                                )
                                processed += 1
                    else:
                        _handle_new_place(avg_lat, avg_lon, streak_start)
                        logger.info(
                            "Retroactive new place at %.5f, %.5f, arrived %s (%.1f min)",
                            avg_lat, avg_lon, streak_start, duration
                        )
                        processed += 1

        # Advance past this cluster (whether a stay or a short stop)
        i = j if j > i else i + 1

    return processed


# ---------------------------------------------------------------------------
# Prefect tasks and flow
# ---------------------------------------------------------------------------

@task(cache_policy=NO_CACHE)
def load_and_scan() -> dict:
    logger = get_run_logger()

    cursor = _read_marker()
    if cursor:
        logger.info("Retroactive scan resuming from cursor: %s", cursor)
    else:
        logger.info("No marker found — scanning full location history")

    points = _get_points_from(cursor)
    logger.info("Loaded %d points for scan", len(points))

    if not points:
        logger.info("No points to scan")
        return {"points_scanned": 0, "stays_processed": 0, "cursor": cursor}

    stays = _scan_for_stays(points, logger)
    logger.info("Scan complete: %d stay(s) processed from %d points", stays, len(points))

    new_cursor = points[-1]['timestamp']
    _write_marker(new_cursor)
    logger.info("Marker updated to %s", new_cursor)

    return {
        "points_scanned": len(points),
        "stays_processed": stays,
        "cursor": new_cursor,
    }


@task(cache_policy=NO_CACHE)
def run_departure_check() -> dict:
    """Close any open visits whose departure is now confirmed by historical data."""
    logger = get_run_logger()
    logger.info("Running departure check on all open visits")
    check_departure()
    return {"status": "completed"}


@flow(name="Retroactive Location Scan")
def retroactive_location_scan_flow():
    scan_result     = load_and_scan()
    departure_result = run_departure_check()

    result = {
        "points_scanned":  scan_result["points_scanned"],
        "stays_processed": scan_result["stays_processed"],
        "cursor":          scan_result["cursor"],
        "departure_check": departure_result["status"],
    }
    record_flow_result(result)
    return result
