import logging

from config.general import LOCATION_CHANGE_RADIUS_M, LOCATION_MINIMUM_POINTS, LOCATION_STATIONARITY_RADIUS_M, LOCATION_STAY_DURATION_MINS
from triggers.dispatch import dispatch, haversine_m
from database.connection import get_conn, to_iso_str
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

def init() -> None:

    with get_conn() as conn:

        conn.execute("""
            CREATE TABLE IF NOT EXISTS known_places (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                lat         REAL NOT NULL,
                lon         REAL NOT NULL,
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
    anchor_lat = points[0]['lat']
    anchor_lon = points[0]['lon']
    for p in points[1:]:
        if haversine_m(anchor_lat, anchor_lon, p['lat'], p['lon']) > LOCATION_STATIONARITY_RADIUS_M:
            return None

    avg_lat = sum(p['lat'] for p in points) / len(points)
    avg_lon = sum(p['lon'] for p in points) / len(points)
    return avg_lat, avg_lon

def is_new_location(lat, lon):
    with get_conn() as conn:
        rows = conn.execute("""
        SELECT lat, lon FROM known_places
        """)
    result = rows.fetchall()
    return all(
        haversine_m(lat, lon, r['lat'], r['lon']) > LOCATION_CHANGE_RADIUS_M
        for r in result
    ) # Returns True for empty result, which is correct as no known places is valid

def run():
    centroid = compute_centroid(get_most_recent_points())
    if centroid is None:
        return
    
    lat, lon = centroid
    now = datetime.now(timezone.utc)

    if is_new_location(lat, lon):
        with get_conn() as conn:
            conn.execute("""
            INSERT INTO known_places (lat, lon, first_seen)
            VALUES (?, ?, ?)
            """, (lat, lon, to_iso_str(now)))
        logger.info("New location detected at %.5f, %.5f", lat, lon)
        dispatch(trigger="location_change", 
                 payload={"lat": lat, "lon": lon, "first_seen": to_iso_str(now)}, 
                 cooldown_hours = 1,
                 noti_title="📍 New Location Discovered",
                 noti_body=f"Discovered at {lat:.3f}, {lon:.3f}. Tap here to add a Journal entry.")
    