import logging

from triggers.dispatch import dispatch
from util import haversine_m
from config.general import LOCATION_CHANGE_RADIUS_M, LOCATION_MINIMUM_POINTS, LOCATION_STATIONARITY_RADIUS_M, LOCATION_STAY_DURATION_MINS
from database.location.geocoding import get_place_id, insert_geocode, reverse_geocode
from database.location.known_places.table import table as known_places_table, KnownPlaceRecord
from database.connection import get_conn, to_iso_str
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)


def label_place(place_id: int, label: str) -> bool:
    return known_places_table.label_place(place_id, label)

def get_most_recent_points():
    now = datetime.now(timezone.utc)
    cutoff = to_iso_str(now - timedelta(minutes=LOCATION_STAY_DURATION_MINS))

    with get_conn(read_only=True) as conn:
        return conn.execute("""
        SELECT * FROM location_unified
        WHERE timestamp >= ?
        ORDER BY timestamp DESC
        """, (cutoff,)).fetchall()

def compute_centroid(points):
    if len(points) < LOCATION_MINIMUM_POINTS:
        return None

    anchor_lat = points[0]['latitude']
    anchor_lon = points[0]['longitude']
    for p in points[1:]:
        if haversine_m(anchor_lat, anchor_lon, p['latitude'], p['longitude']) > LOCATION_STATIONARITY_RADIUS_M:
            return None

    avg_lat = sum(p['latitude'] for p in points) / len(points)
    avg_lon = sum(p['longitude'] for p in points) / len(points)
    earliest = min(p['timestamp'] for p in points)  # earliest point in the cluster
    return avg_lat, avg_lon, earliest

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

def run():
    check_departure() 

    centroid = compute_centroid(get_most_recent_points())
    if centroid is None:
        return
    
    lat, lon, arrived_at = centroid
    now = to_iso_str(datetime.now(timezone.utc))
    nearest = get_nearest_known_place(lat, lon)

    if nearest is None:
        place_id = known_places_table.insert(KnownPlaceRecord(
            latitude=lat, longitude=lon, first_seen=arrived_at,
        ))
        visit_id = known_places_table.insert_visit(place_id, arrived_at)
        known_places_table.set_current_visit(place_id, visit_id)

        logger.important("New location detected at %.5f, %.5f", lat, lon)

        address = get_address(lat, lon)
        name = address.get("city") or address.get("suburb") or address.get("road") or address.get("region") or f"{lat:.3f}, {lon:.3f}"

        dispatch(trigger="location_change",
                 payload={"lat": lat, "lon": lon, "first_seen": now},
                 cooldown_hours=1,
                 noti_title="📍 New Location Discovered",
                 noti_body=f"Discovered near {name}. Tap here to add a label.")
    else:
        place_id = nearest['id']
        with get_conn(read_only=True) as conn:
            row = conn.execute("""
                SELECT current_visit_id, label FROM known_places WHERE id = ?
            """, (place_id,)).fetchone()

        name = row['label'] if row['label'] else f"known place {place_id}"

        if row['current_visit_id'] is not None:
            logger.info(f"Still at {name}, no action needed")
            return

        visit_id = known_places_table.insert_visit(place_id, arrived_at)
        logger.important(f"Return visit to {name}")
        known_places_table.increment_visit_count(place_id, arrived_at, visit_id)

def get_current_visit():
    """Returns (visit_id, place_id, lat, lon, arrived_at) for the open visit, or None."""
    with get_conn(read_only=True) as conn:
        return conn.execute("""
            SELECT pv.id, pv.place_id, kp.latitude, kp.longitude, pv.arrived_at
            FROM place_visits pv
            JOIN known_places kp ON kp.id = pv.place_id
            WHERE pv.departed_at IS NULL
            ORDER BY pv.arrived_at DESC
            LIMIT 1
        """).fetchone()

def check_departure():
    visit = get_current_visit()
    if visit is None:
        return

    visit_id, place_id, kp_lat, kp_lon, arrived_at = visit

    points = get_most_recent_points()
    if not points:
        return

    last_in_radius = None
    for p in points:
        if haversine_m(kp_lat, kp_lon, p['latitude'], p['longitude']) <= LOCATION_CHANGE_RADIUS_M:
            if last_in_radius is None or p['timestamp'] > last_in_radius:
                last_in_radius = p['timestamp']

    if last_in_radius is not None:
        return  # still at location

    # All recent points outside radius — use timestamp of last in-radius point as departure time
    # Fall back to arrived_at + 1 min if somehow no in-radius point found in history
    departed_at = get_last_in_radius_timestamp(kp_lat, kp_lon, arrived_at)
    if departed_at is None:
        departed_at = to_iso_str(datetime.now(timezone.utc))  # fallback only

    arrived_dt = datetime.fromisoformat(arrived_at)
    departed_dt = datetime.fromisoformat(departed_at)
    duration_mins = int((departed_dt - arrived_dt).total_seconds() / 60)

    known_places_table.close_visit(visit_id, place_id, departed_at, duration_mins)

    with get_conn(read_only=True) as conn:
        row = conn.execute("SELECT label FROM known_places WHERE id = ?", (place_id,)).fetchone()
    name = row['label'] if row['label'] else f"known place {place_id}"

    logger.important(f"Departed {name} after {duration_mins} mins")

def get_nearest_known_place(lat, lon):
    """Returns the nearest known place row if within radius, else None."""
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

def get_last_in_radius_timestamp(kp_lat, kp_lon, arrived_at: str):
    """Find the most recent location point within radius since the visit started."""
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