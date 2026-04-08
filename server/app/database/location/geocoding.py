"""
database/location/geocoding.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Nominatim reverse-geocoding helpers and place-id lookup/creation.

get_place_id()   — upsert a places row and return its id (used at every
                   location insert point to snap lat/lon to the nearest
                   0.001° grid cell).
insert_geocode() — update a places row with a geocoding result from
                   reverse_geocode().
reverse_geocode() — call the Nominatim API for a single lat/lon.
batch_geocode()   — geocode a list of coordinates with a 1s rate-limit delay.
"""

from datetime import datetime, timezone
import time

import requests

from database.connection import get_conn, to_iso_str


def reverse_geocode(lat: float, lon: float) -> dict:
    resp = requests.get(
        "https://nominatim.openstreetmap.org/reverse",
        params={"lat": lat, "lon": lon, "format": "jsonv2"},
        headers={"User-Agent": "TravelNet/1.0 dan@travelnet.dev"},  # required
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


def batch_geocode(coords: list[tuple[float, float]]) -> list[dict]:
    """Batch geocode a list of (lat, lon) tuples using Nominatim's bulk endpoint."""
    locations = []
    for lat, lon in coords:
        try:
            loc = reverse_geocode(lat, lon)
            locations.append(loc)
            time.sleep(1)
        except Exception as e:
            print(f"Error geocoding {lat}, {lon}: {e}")
            locations.append({})
    return locations


def insert_geocode(place_id: int, geocode: dict) -> None:
    with get_conn() as conn:
        conn.execute("""
        UPDATE places SET
            country_code = ?,
            country = ?,
            region = ?,
            city = ?,
            suburb = ?,
            road = ?,
            display_name = ?,
            geocoded_at = ?
        WHERE id = ?
        """, (
            geocode.get("address", {}).get("country_code"),
            geocode.get("address", {}).get("country"),
            geocode.get("address", {}).get("state"),
            geocode.get("address", {}).get("city"),
            geocode.get("address", {}).get("suburb"),
            geocode.get("address", {}).get("road"),
            geocode.get("display_name"),
            to_iso_str(datetime.now(timezone.utc)),
            place_id
        ))


def get_place_id(lat: float, lon: float) -> int | None:
    lat_snap, lon_snap = round(lat, 3), round(lon, 3)
    with get_conn() as conn:
        conn.execute("""
        INSERT OR IGNORE INTO places (lat_snap, lon_snap) VALUES (?, ?)
        """, (lat_snap, lon_snap))
        row = conn.execute("""
        SELECT id FROM places WHERE lat_snap = ? AND lon_snap = ?
        """, (lat_snap, lon_snap)).fetchone()
        return row[0] if row else None
