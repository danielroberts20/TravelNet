import logging

from config.general import LOCATION_CHANGE_RADIUS_M, LOCATION_MINIMUM_POINTS, LOCATION_STATIONARITY_RADIUS_M, LOCATION_STAY_DURATION_MINS
from database.location.util import get_place_id, insert_geocode, reverse_geocode
from triggers.dispatch import dispatch, haversine_m
from database.connection import get_conn, to_iso_str
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

def init() -> None:

    with get_conn() as conn:

        conn.execute("""
            CREATE TABLE IF NOT EXISTS known_places (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                latitude         REAL NOT NULL,
                longitude         REAL NOT NULL,
                first_seen  TEXT NOT NULL,   -- ISO 8601 UTC
                visit_count     INTEGER NOT NULL DEFAULT 0,
                last_visited    TEXT,                          -- ISO 8601 UTC
                total_time_mins INTEGER NOT NULL DEFAULT 0,
                current_visit_id INTEGER REFERENCES place_visits(id),
                label       TEXT             -- optional manual label, e.g. "Hostel, Chiang Mai"
            );""")

def label_place(place_id: int, label: str):
    with get_conn() as conn:
        cursor = conn.execute("""
        UPDATE known_places
        SET label = ?
        WHERE id = ?
        """, (label, place_id))
        return cursor.rowcount > 0

def get_most_recent_points():    
    now = datetime.now(timezone.utc)
    cutoff = to_iso_str(now - timedelta(minutes=LOCATION_STAY_DURATION_MINS))

    with get_conn() as conn:
        rows = conn.execute("""
        SELECT * FROM location_unified
        WHERE timestamp >= ?                    
        ORDER BY timestamp DESC
        """, (cutoff,))
    return rows.fetchall()

def compute_centroid(points):
    if len(points) < LOCATION_MINIMUM_POINTS:
        return None

    # Check spread — if points are too dispersed, we're still moving
    anchor_lat = points[0]['latitude']
    anchor_lon = points[0]['longitude']
    for p in points[1:]:
        if haversine_m(anchor_lat, anchor_lon, p['latitude'], p['longitude']) > LOCATION_STATIONARITY_RADIUS_M:
            return None

    avg_lat = sum(p['latitude'] for p in points) / len(points)
    avg_lon = sum(p['longitude'] for p in points) / len(points)
    return avg_lat, avg_lon

def get_address(lat, lon):
    with get_conn() as conn:
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
    
    lat, lon = centroid
    now = to_iso_str(datetime.now(timezone.utc))
    nearest = get_nearest_known_place(lat, lon)

    if nearest is None:
        with get_conn() as conn:
            cursor = conn.execute("""
                INSERT INTO known_places (latitude, longitude, first_seen, visit_count, last_visited)
                VALUES (?, ?, ?, 1, ?)
            """, (lat, lon, now, now))
            place_id = cursor.lastrowid

            visit_cursor = conn.execute("""
                INSERT INTO place_visits (place_id, arrived_at)
                VALUES (?, ?)
            """, (place_id, now))
            visit_id = visit_cursor.lastrowid

            conn.execute("""
                UPDATE known_places SET current_visit_id = ? WHERE id = ?
            """, (visit_id, place_id))

        logger.important("New location detected at %.5f, %.5f", lat, lon)

        address = get_address(lat, lon)
        name = address.get("city") or address.get("suburb") or address.get("road") or address.get("region") or f"{lat:.3f}, {lon:.3f}"

        dispatch(trigger="location_change", 
                 payload={"lat": lat, "lon": lon, "first_seen": now}, 
                 cooldown_hours = 1,
                 noti_title="📍 New Location Discovered",
                 noti_body=f"Discovered near {name}. Tap here to add a Journal entry.")
    else:
        place_id = nearest['id']
        with get_conn() as conn:
            row = conn.execute("""
                SELECT current_visit_id FROM known_places WHERE id = ?
            """, (place_id,)).fetchone()

            if row['current_visit_id'] is not None:
                logger.info(f"Still at known place {place_id}, no action needed")
                return

            visit_cursor = conn.execute("""
                INSERT INTO place_visits (place_id, arrived_at)
                VALUES (?, ?)
            """, (place_id, now))
            logger.important(f"Return visit to known place {place_id}")
            conn.execute("""
                UPDATE known_places
                SET visit_count = visit_count + 1,
                    last_visited = ?,
                    current_visit_id = ?
                WHERE id = ?
            """, (now, visit_cursor.lastrowid, place_id))

def get_current_visit():
    """Returns (visit_id, place_id, lat, lon, arrived_at) for the open visit, or None."""
    with get_conn() as conn:
        row = conn.execute("""
            SELECT pv.id, pv.place_id, kp.latitude, kp.longitude, pv.arrived_at
            FROM place_visits pv
            JOIN known_places kp ON kp.id = pv.place_id
            WHERE pv.departed_at IS NULL
            ORDER BY pv.arrived_at DESC
            LIMIT 1
        """).fetchone()
    return row

def check_departure():
    visit = get_current_visit()
    if visit is None:
        return

    visit_id, place_id, kp_lat, kp_lon, arrived_at = visit

    points = get_most_recent_points()
    if not points:
        return

    # If any recent point is still within radius, we haven't departed
    for p in points:
        if haversine_m(kp_lat, kp_lon, p['latitude'], p['longitude']) <= LOCATION_CHANGE_RADIUS_M:
            return

    # All recent points are outside radius — departure confirmed
    now = datetime.now(timezone.utc)
    departed_at = to_iso_str(now)
    arrived_dt = datetime.fromisoformat(arrived_at)
    duration_mins = int((now - arrived_dt).total_seconds() / 60)

    with get_conn() as conn:
        conn.execute("""
            UPDATE place_visits
            SET departed_at = ?, duration_mins = ?
            WHERE id = ?
        """, (departed_at, duration_mins, visit_id))

        conn.execute("""
            UPDATE known_places
            SET total_time_mins = total_time_mins + ?,
                current_visit_id = NULL
            WHERE id = ?
        """, (duration_mins, place_id))

    logger.important("Departed known place %d after %d mins", place_id, duration_mins)

def get_nearest_known_place(lat, lon):
    """Returns the nearest known place row if within radius, else None."""
    with get_conn() as conn:
        rows = conn.execute("SELECT id, latitude, longitude FROM known_places").fetchall()
    closest = None
    closest_dist = float('inf')
    for r in rows:
        dist = haversine_m(lat, lon, r['latitude'], r['longitude'])
        if dist <= LOCATION_CHANGE_RADIUS_M and dist < closest_dist:
            closest = r
            closest_dist = dist
    return closest