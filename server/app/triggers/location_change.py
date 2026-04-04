import logging

from config.general import LOCATION_CHANGE_RADIUS_M, LOCATION_MINIMUM_POINTS, LOCATION_STATIONARITY_RADIUS_M, LOCATION_STAY_DURATION_MINS
from database.location.util import insert_geocode, reverse_geocode
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

def is_new_location(lat, lon):
    with get_conn() as conn:
        rows = conn.execute("""
        SELECT latitude, longitude FROM known_places
        """)
    result = rows.fetchall()
    return all(
        haversine_m(lat, lon, r['latitude'], r['longitude']) > LOCATION_CHANGE_RADIUS_M
        for r in result
    ) # Returns True for empty result, which is correct as no known places is valid

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
    centroid = compute_centroid(get_most_recent_points())
    if centroid is None:
        return
    
    lat, lon = centroid
    now = datetime.now(timezone.utc)

    if is_new_location(lat, lon):
        with get_conn() as conn:
            conn.execute("""
            INSERT INTO known_places (latitude, longitude, first_seen)
            VALUES (?, ?, ?)
            """, (lat, lon, to_iso_str(now)))
        logger.info("New location detected at %.5f, %.5f", lat, lon)

        address = get_address(lat, lon)
        name = address.get("city") or address.get("suburb") or address.get("road") or address.get("region") or f"{lat:.3f}, {lon:.3f}"

        dispatch(trigger="location_change", 
                 payload={"lat": lat, "lon": lon, "first_seen": to_iso_str(now)}, 
                 cooldown_hours = 1,
                 noti_title="📍 New Location Discovered",
                 noti_body=f"Discovered near {name}. Tap here to add a Journal entry.")
    