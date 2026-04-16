import logging

from triggers.dispatch import dispatch
from util import haversine_m
from config.general import (
    LOCATION_CHANGE_RADIUS_M,
    LOCATION_MINIMUM_POINTS,
    LOCATION_STATIONARITY_RADIUS_M,
    LOCATION_STAY_DURATION_MINS,
    LOCATION_REVISIT_DURATION_MINS,
    LOCATION_DEPARTURE_CONFIRMATION_MINS,
    LOCATION_STREAK_POINT_LIMIT,
)
from database.location.geocoding import insert_geocode, reverse_geocode
from database.location.known_places.table import table as known_places_table, KnownPlaceRecord
from database.connection import get_conn, to_iso_str
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)


def label_place(place_id: int, label: str) -> bool:
    return known_places_table.label_place(place_id, label)


# ---------------------------------------------------------------------------
# Streak detection (Fix 1)
# ---------------------------------------------------------------------------

def get_stationary_streak(limit: int = None):
    """Return the current stationary streak of GPS points, or None.

    Queries the most recent `limit` points from location_unified ordered by
    timestamp DESC, anchors on the most recent point, and walks backward
    accumulating points that fall within LOCATION_STATIONARITY_RADIUS_M of
    that anchor.  Stops at the first point outside the radius.

    Returns (centroid_lat, centroid_lon, streak_start, streak_end, point_count)
    where streak_start / streak_end are ISO timestamp strings, or None if
    there are fewer than LOCATION_MINIMUM_POINTS in the streak.

    Duration must be checked by the caller against the appropriate threshold:
      - known place arrival : LOCATION_REVISIT_DURATION_MINS
      - new place discovery : LOCATION_STAY_DURATION_MINS
    """
    if limit is None:
        limit = LOCATION_STREAK_POINT_LIMIT

    with get_conn(read_only=True) as conn:
        points = conn.execute("""
            SELECT timestamp, latitude, longitude
            FROM location_unified
            ORDER BY timestamp DESC
            LIMIT ?
        """, (limit,)).fetchall()

    if not points:
        return None

    anchor_lat = points[0]['latitude']
    anchor_lon = points[0]['longitude']

    streak = []
    for p in points:
        if haversine_m(anchor_lat, anchor_lon, p['latitude'], p['longitude']) <= LOCATION_STATIONARITY_RADIUS_M:
            streak.append(p)
        else:
            break  # first point outside radius ends the contiguous streak

    if len(streak) < LOCATION_MINIMUM_POINTS:
        return None

    # streak is in DESC order; oldest is last
    streak_end   = streak[0]['timestamp']   # most recent
    streak_start = streak[-1]['timestamp']  # oldest

    avg_lat = sum(p['latitude']  for p in streak) / len(streak)
    avg_lon = sum(p['longitude'] for p in streak) / len(streak)

    return avg_lat, avg_lon, streak_start, streak_end, len(streak)


def _streak_duration_mins(streak_start: str, streak_end: str) -> float:
    """Return the duration of a streak in minutes using point timestamps."""
    start_dt = datetime.fromisoformat(streak_start)
    end_dt   = datetime.fromisoformat(streak_end)
    return (end_dt - start_dt).total_seconds() / 60


# ---------------------------------------------------------------------------
# Departure detection (Fix 2 + 3)
# ---------------------------------------------------------------------------

def get_all_open_visits():
    """Return all place_visits rows where departed_at IS NULL, joined to known_places."""
    with get_conn(read_only=True) as conn:
        return conn.execute("""
            SELECT pv.id, pv.place_id, kp.latitude, kp.longitude, pv.arrived_at
            FROM place_visits pv
            JOIN known_places kp ON kp.id = pv.place_id
            WHERE pv.departed_at IS NULL
            ORDER BY pv.arrived_at DESC
        """).fetchall()


def get_first_point_after(timestamp: str):
    """Return the first location_unified row with timestamp strictly after the given value."""
    with get_conn(read_only=True) as conn:
        return conn.execute("""
            SELECT timestamp FROM location_unified
            WHERE timestamp > ?
            ORDER BY timestamp ASC
            LIMIT 1
        """, (timestamp,)).fetchone()


def get_last_in_radius_timestamp(kp_lat: float, kp_lon: float, arrived_at: str):
    """Find the most recent location point within LOCATION_CHANGE_RADIUS_M since the visit started."""
    with get_conn(read_only=True) as conn:
        rows = conn.execute("""
            SELECT timestamp, latitude, longitude FROM location_unified
            WHERE timestamp >= ?
            ORDER BY timestamp DESC
        """, (arrived_at,)).fetchall()
    for r in rows:
        if haversine_m(kp_lat, kp_lon, r['latitude'], r['longitude']) <= LOCATION_CHANGE_RADIUS_M:
            return r['timestamp']
    return None


def check_departure():
    """Check all open visits for departure and close any that are confirmed.

    Departure is confirmed when:
      1. The last in-radius point is older than LOCATION_DEPARTURE_CONFIRMATION_MINS.
      2. At least one location point exists after that last in-radius point,
         confirming that tracking continued and the absence is real departure,
         not a tracking gap.

    departed_at is set to the last in-radius timestamp — the accurate moment
    the place was last visited, not the moment of detection.
    """
    open_visits = get_all_open_visits()
    if not open_visits:
        return

    confirmation_cutoff = to_iso_str(
        datetime.now(timezone.utc) - timedelta(minutes=LOCATION_DEPARTURE_CONFIRMATION_MINS)
    )

    for visit in open_visits:
        visit_id, place_id, kp_lat, kp_lon, arrived_at = visit

        last_in = get_last_in_radius_timestamp(kp_lat, kp_lon, arrived_at)

        if last_in is None:
            # No in-radius point found since arrived_at — stale/orphaned visit.
            # Use arrived_at itself as a safe fallback so we still close it.
            last_in = arrived_at

        if last_in >= confirmation_cutoff:
            continue  # still within the confirmation window

        if get_first_point_after(last_in) is None:
            continue  # no data after last_in — can't distinguish departure from tracking gap

        # Confirmed departure
        arrived_dt  = datetime.fromisoformat(arrived_at)
        departed_dt = datetime.fromisoformat(last_in)
        duration_mins = max(0, int((departed_dt - arrived_dt).total_seconds() / 60))

        known_places_table.close_visit(visit_id, place_id, last_in, duration_mins)

        with get_conn(read_only=True) as conn:
            row = conn.execute(
                "SELECT label FROM known_places WHERE id = ?", (place_id,)
            ).fetchone()
        name = row['label'] if row['label'] else f"known place {place_id}"
        logger.info("Departed %s after %d mins", name, duration_mins)


# ---------------------------------------------------------------------------
# Idempotency helper (used by retroactive scanner)
# ---------------------------------------------------------------------------

def visit_exists(place_id: int, arrived_at: str, tolerance_mins: int = 5) -> bool:
    """Return True if a visit for place_id already exists within tolerance_mins of arrived_at.

    Used by the retroactive scanner to avoid creating duplicate visit records
    when the same historical window is processed more than once.
    """
    with get_conn(read_only=True) as conn:
        row = conn.execute("""
            SELECT id FROM place_visits
            WHERE place_id = ?
              AND ABS(
                  (julianday(arrived_at) - julianday(?)) * 24 * 60
              ) <= ?
        """, (place_id, arrived_at, tolerance_mins)).fetchone()
    return row is not None


# ---------------------------------------------------------------------------
# Arrival helpers
# ---------------------------------------------------------------------------

def get_address(lat, lon):
    with get_conn(read_only=True) as conn:
        row = conn.execute("""
        SELECT country_code, country, region,
               city, suburb, road, display_name, geocoded_at FROM places
        WHERE lat_snap = ROUND(?, 3) AND lon_snap = ROUND(?, 3)
        """, (lat, lon)).fetchone()
    if row:
        return {
            "country_code": row["country_code"],
            "country": row["country"],
            "region": row["region"],
            "city": row["city"],
            "suburb": row["suburb"],
            "road": row["road"],
            "display_name": row["display_name"]
        }

    geocode = reverse_geocode(lat, lon)
    insert_geocode(lat, lon, geocode)
    return {
        "country_code": geocode.get("address", {}).get("country_code"),
        "country": geocode.get("address", {}).get("country"),
        "region": geocode.get("address", {}).get("state"),
        "city": geocode.get("address", {}).get("city"),
        "suburb": geocode.get("address", {}).get("suburb"),
        "road": geocode.get("address", {}).get("road"),
        "display_name": geocode.get("display_name")
    }


def get_nearest_known_place(lat, lon):
    """Return the nearest known place row if within LOCATION_CHANGE_RADIUS_M, else None."""
    with get_conn(read_only=True) as conn:
        rows = conn.execute("SELECT id, latitude, longitude FROM known_places").fetchall()
    closest = None
    closest_dist = float('inf')
    for r in rows:
        dist = haversine_m(lat, lon, r['latitude'], r['longitude'])
        if dist <= LOCATION_CHANGE_RADIUS_M and dist < closest_dist:
            closest = r
            closest_dist = dist
    return closest


def _handle_known_place(nearest, arrived_at: str) -> bool:
    """Record a visit (or note an ongoing one) for a known place.

    Returns True if a new visit was opened, False if a visit is already active.
    """
    place_id = nearest['id']
    with get_conn(read_only=True) as conn:
        row = conn.execute(
            "SELECT current_visit_id, label FROM known_places WHERE id = ?",
            (place_id,),
        ).fetchone()

    name = row['label'] if row['label'] else f"known place {place_id}"

    if row['current_visit_id'] is not None:
        logger.info("Still at %s, no action needed", name)
        return False

    visit_id = known_places_table.insert_visit(place_id, arrived_at)
    logger.info("Return visit to %s", name)
    known_places_table.increment_visit_count(place_id, arrived_at, visit_id)
    return True


def _handle_new_place(lat: float, lon: float, arrived_at: str) -> None:
    """Create a new known place and its first visit, then fire the discovery notification."""
    place_id = known_places_table.insert(KnownPlaceRecord(
        latitude=lat, longitude=lon, first_seen=arrived_at,
    ))
    visit_id = known_places_table.insert_visit(place_id, arrived_at)
    known_places_table.set_current_visit(place_id, visit_id)

    logger.info("New location detected at %.5f, %.5f", lat, lon)

    address = get_address(lat, lon)
    name = (address.get("city") or address.get("suburb") or address.get("road")
            or address.get("region") or f"{lat:.3f}, {lon:.3f}")
    now = to_iso_str(datetime.now(timezone.utc))

    dispatch(trigger="location_change",
             payload={"lat": lat, "lon": lon, "first_seen": now},
             cooldown_hours=1,
             noti_title="📍 New Location Discovered",
             noti_body=f"Discovered near {name}. Tap here to add a label.")


# ---------------------------------------------------------------------------
# Core detection logic (shared by real-time trigger and retroactive scan)
# ---------------------------------------------------------------------------

def detect_arrival(streak=None):
    """Run arrival detection against the current stationary streak.

    Accepts a pre-computed streak tuple so the retroactive scanner can pass
    in a streak built from a historical point window without re-querying.
    If streak is None the live streak is computed from the DB.

    Returns True if a visit was opened (new or revisit), False otherwise.
    """
    if streak is None:
        streak = get_stationary_streak()
    if streak is None:
        return False

    lat, lon, streak_start, streak_end, point_count = streak
    duration = _streak_duration_mins(streak_start, streak_end)

    nearest = get_nearest_known_place(lat, lon)

    if nearest is not None:
        # Known place — short arrival window
        if duration >= LOCATION_REVISIT_DURATION_MINS:
            _handle_known_place(nearest, streak_start)
            return True
    else:
        # Unknown place — long arrival window
        if duration >= LOCATION_STAY_DURATION_MINS:
            _handle_new_place(lat, lon, streak_start)
            return True

    return False


# ---------------------------------------------------------------------------
# Real-time trigger entry point
# ---------------------------------------------------------------------------

def run():
    check_departure()
    detect_arrival()
