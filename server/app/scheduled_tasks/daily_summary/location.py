"""
scheduled_tasks/daily_summary/location.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Owns the location and movement columns of daily_summary.

Includes the locality fallback logic: prefers `city` but falls back to
`town` (e.g. Broadchurch) → `village` → `municipality` → `hamlet` →
`suburb`. The `places` table doesn't currently have all these fields —
we read from `display_name` as a backup where needed.
"""
from config.editable import load_overrides
load_overrides()

from datetime import datetime, timedelta

from prefect import flow
from prefect.logging import get_run_logger

from notifications import record_flow_result
from scheduled_tasks.daily_summary.base import Domain, closed_after
from config.general import WAKING_HOURS, EXPECTED_POINTS_PER_HOUR

# ---------------------------------------------------------------------------
# Compute function
# ---------------------------------------------------------------------------

def compute_location_data(conn, ctx: dict) -> dict:
    data = {}
    data.update(_dominant_place(conn, ctx))
    data.update(_movement(conn, ctx))
    return data


def _dominant_place(conn, ctx: dict) -> dict:
    """
    Most-visited place for the day. Returns country/region/city metadata
    with `city` sourced from the pre-computed `locality` column, falling
    back to the raw `city` field for places geocoded before the migration.
    Also returns the dominant known_place_id based on the longest overlapping
    place_visit within the day's UTC window.
    """
    row = conn.execute("""
        SELECT place_id, COUNT(*) AS c
        FROM location_unified
        WHERE timestamp >= ? AND timestamp < ?
          AND place_id IS NOT NULL
        GROUP BY place_id
        ORDER BY c DESC LIMIT 1
    """, (ctx["utc_start"], ctx["utc_end"])).fetchone()

    if not row:
        return {
            "country_code": None, "country": None, "region": None,
            "city": None, "dominant_place_id": None,
            "dominant_known_place_id": None,
        }

    place_id = row["place_id"]
    place = conn.execute("""
        SELECT country_code, country, region, locality, city
        FROM places WHERE id = ?
    """, (place_id,)).fetchone()

    if not place:
        return {
            "country_code": None, "country": None, "region": None,
            "city": None, "dominant_place_id": place_id,
            "dominant_known_place_id": None,
        }

    known = conn.execute("""
        SELECT
            pv.known_place_id,
            SUM(
                CAST(
                    (strftime('%s', MIN(COALESCE(pv.departed_at, ?), ?)) -
                     strftime('%s', MAX(pv.arrived_at, ?)))
                AS REAL) / 60.0
            ) AS overlap_mins
        FROM place_visits pv
        WHERE pv.arrived_at < ?
          AND (pv.departed_at IS NULL OR pv.departed_at > ?)
        GROUP BY pv.known_place_id
        ORDER BY overlap_mins DESC
        LIMIT 1
    """, (
        ctx["utc_end"], ctx["utc_end"],   # COALESCE fallback, MIN cap
        ctx["utc_start"],                  # MAX floor
        ctx["utc_end"],                    # arrived before day end
        ctx["utc_start"],                  # departed after day start
    )).fetchone()

    best_city = place["locality"] or place["city"]
    return {
        "country_code":            place["country_code"],
        "country":                 place["country"],
        "region":                  place["region"],
        "city":                    best_city,
        "dominant_place_id":       place_id,
        "dominant_known_place_id": known["known_place_id"] if known else None,
    }

def _movement(conn, ctx: dict) -> dict:
    loc = conn.execute("""
        SELECT COUNT(*) AS n FROM location_unified
        WHERE timestamp >= ? AND timestamp < ?
    """, (ctx["utc_start"], ctx["utc_end"])).fetchone()

    ovl = conn.execute("""
        SELECT COUNT(*) AS n FROM location_overland_cleaned
        WHERE timestamp >= ? AND timestamp < ?
    """, (ctx["utc_start"], ctx["utc_end"])).fetchone()

    distinct = conn.execute("""
        SELECT COUNT(DISTINCT place_id) AS n FROM location_unified
        WHERE timestamp >= ? AND timestamp < ?
          AND place_id IS NOT NULL
    """, (ctx["utc_start"], ctx["utc_end"])).fetchone()

    flights = conn.execute("""
        SELECT COUNT(*) AS n FROM flights
        WHERE (departed_at >= ? AND departed_at < ?)
           OR (arrived_at  >= ? AND arrived_at  < ?)
    """, (ctx["utc_start"], ctx["utc_end"],
          ctx["utc_start"], ctx["utc_end"])).fetchone()

    # Coverage: cleaned overland points during waking hours vs expected
    waking_start = _shift_utc(ctx["utc_start"], hours=6)
    ovl_waking = conn.execute("""
        SELECT COUNT(*) AS n FROM location_overland_cleaned
        WHERE timestamp >= ? AND timestamp < ?
    """, (waking_start, ctx["utc_end"])).fetchone()

    expected = WAKING_HOURS * EXPECTED_POINTS_PER_HOUR
    coverage = round(min(100.0, ovl_waking["n"] / expected * 100), 2) \
        if expected else None
    
    new_places = conn.execute("""
        SELECT COUNT(*) AS n FROM known_places 
        WHERE first_seen >= ? AND first_seen < ?
    """, (ctx["utc_start"], ctx["utc_end"])).fetchone()

    return {
        "location_points":       loc["n"],
        "overland_points":       ovl["n"],
        "overland_coverage_pct": coverage,
        "distinct_places":       distinct["n"],
        "new_places_visited":    new_places["n"],
        "was_in_transit":        1 if flights["n"] > 0 else 0,
    }


def _shift_utc(utc_ts: str, hours: int) -> str:
    dt = datetime.fromisoformat(utc_ts.replace("Z", "+00:00"))
    return (dt + timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# Domain spec
# ---------------------------------------------------------------------------

LOCATION_DOMAIN = Domain(
    name="location",
    columns=frozenset({
        "country_code", "country", "region", "city", "dominant_place_id",
        "dominant_known_place_id", "location_points", "overland_points", 
        "overland_coverage_pct", "distinct_places", "new_places_visited", 
        "was_in_transit",
    }),
    completeness_flag="location_complete",
    compute_fn=compute_location_data,
    # Location is near-realtime — 1 day of buffer is plenty for any
    # straggler Shortcuts uploads.
    completeness_predicate=closed_after(1),
)


@flow(
    name="Compute Daily Summary — Location"
)
def compute_location_flow(local_date: str) -> dict:
    logger = get_run_logger()
    data = LOCATION_DOMAIN.upsert_for_date(local_date)
    logger.info(f"{local_date}: location domain upserted "
                f"(country={data.get('country')}, city={data.get('city')}, "
                f"points={data.get('location_points')})")
    result = {"local_date": local_date, **data}
    record_flow_result(result)
    return result