"""
scheduled_tasks/flag_location_noise.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Retrospectively apply location noise flags to existing overland points.

Tier 1: horizontal_accuracy > 100m (stateless, run on any unflagged point)
Tier 2: displacement spike detection (requires prev/next context — skips
        the most recent N points where the trailing window is incomplete)

Run via Prefect
"""

from config.editable import load_overrides
load_overrides()

from prefect import task, flow
from prefect.logging import get_run_logger

from config.general import (
    LOCATION_NOISE_ACCURACY_THRESHOLD, 
    TIER2_DISPLACEMENT_M, 
    TIER2_TRAILING_SKIP, 
    TIER2_RETURN_M, 
    TIER2_WINDOW_S
)
from database.connection import get_conn
from database.location.noise.table import table as noise_table, LocationNoiseRecord
from util import haversine_m, parse_ts


@task
def flag_tier1_noise(conn) -> int:
    """Flag unflagged points where horizontal_accuracy > threshold."""
    logger = get_run_logger()
    rows = conn.execute("""
        SELECT o.id, o.horizontal_accuracy
        FROM location_overland o
        WHERE o.horizontal_accuracy > ?
        AND NOT EXISTS (
            SELECT 1 FROM location_noise n WHERE n.overland_id = o.id
        )
    """, (LOCATION_NOISE_ACCURACY_THRESHOLD,)).fetchall()

    for row in rows:
        noise_table.insert(LocationNoiseRecord(
            overland_id=row["id"],
            tier=1,
            reason="accuracy_threshold",
        ))

    logger.info("Flagged %d location point(s) as tier-1 noise", len(rows))
    return len(rows)

@task
def flag_tier2_noise(conn) -> int:
    """
    Flag unflagged points where the point displaces far from its predecessor
    and the following point returns close to the pre-spike position —
    the out-and-back signature of a bad GPS fix.

    Skips points already flagged by tier 1, and skips the trailing
    TIER2_TRAILING_SKIP points where the next-point context may be absent.
    """
    rows = conn.execute("""
        SELECT
            o.id,
            o.timestamp,
            o.latitude,
            o.longitude,
            LAG(o.latitude)   OVER (ORDER BY o.timestamp) AS prev_lat,
            LAG(o.longitude)  OVER (ORDER BY o.timestamp) AS prev_lon,
            LEAD(o.latitude)  OVER (ORDER BY o.timestamp) AS next_lat,
            LEAD(o.longitude) OVER (ORDER BY o.timestamp) AS next_lon,
            LAG(o.timestamp)  OVER (ORDER BY o.timestamp) AS prev_ts,
            LEAD(o.timestamp) OVER (ORDER BY o.timestamp) AS next_ts
        FROM location_overland o
        WHERE NOT EXISTS (
            SELECT 1 FROM location_noise n WHERE n.overland_id = o.id
        ) AND o.timestamp > COALESCE(
            (SELECT datetime(MAX(o2.timestamp), '-5 minutes')
            FROM location_overland o2
            INNER JOIN location_noise n ON n.overland_id = o2.id),
            '1970-01-01T00:00:00Z'
        )
        ORDER BY o.timestamp
    """).fetchall()

    # Drop trailing rows where next context doesn't exist yet
    rows = rows[:-TIER2_TRAILING_SKIP] if len(rows) > TIER2_TRAILING_SKIP else []

    if len(rows) < 3:
        return 0

    flagged = 0
    for row in rows:
        # Skip first row (no prev)
        if row["prev_lat"] is None or row["next_lat"] is None:
            continue

        dist_from_prev = haversine_m(
            row["prev_lat"], row["prev_lon"],
            row["latitude"], row["longitude"],
        )
        dist_return = haversine_m(
            row["next_lat"], row["next_lon"],
            row["prev_lat"], row["prev_lon"],
        )

        # Time window check
        window_s = (parse_ts(row["next_ts"]) - parse_ts(row["prev_ts"])).total_seconds()

        if (
            dist_from_prev > TIER2_DISPLACEMENT_M
            and dist_return  < TIER2_RETURN_M
            and window_s     < TIER2_WINDOW_S
        ):
            noise_table.insert(LocationNoiseRecord(
                overland_id=row["id"],
                tier=2,
                reason="displacement_spike",
            ))
            flagged += 1

    return flagged


@flow(name="Identify Location Noise")
def flag_location_noise_flow():
    with get_conn() as conn:
        t1 = flag_tier1_noise(conn)
        t2 = flag_tier2_noise(conn)
    return {"tier1": t1,
            "tier2": t2}
