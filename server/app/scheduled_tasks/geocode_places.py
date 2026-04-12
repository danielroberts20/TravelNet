from config.editable import load_overrides
load_overrides()

import logging
from timezonefinder import TimezoneFinder

from prefect import task, flow

from config.settings import settings
from database.connection import get_conn
from database.location.geocoding import batch_geocode, insert_geocode

logger = logging.getLogger(__name__)


@task
def fetch_uncoded_places() -> list[tuple[int, float, float]]:
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT id, lat_snap, lon_snap FROM places
            WHERE geocoded_at IS NULL
              AND lat_snap IS NOT NULL
              AND lon_snap IS NOT NULL
        """).fetchall()
    logger.info(f"Found {len(rows)} places to geocode")
    return [(row[0], row[1], row[2]) for row in rows]


@task
def geocode_batch(coords: list[tuple[float, float]]) -> list[dict]:
    return batch_geocode(coords)


@task
def store_geocodes(rows: list[tuple[int, float, float]], geocodes: list[dict]):
    """Insert geocode results and timezones in a single connection."""
    tf = TimezoneFinder()
    with get_conn() as conn:
        for (place_id, lat, lon), geocode in zip(rows, geocodes):
            insert_geocode(place_id, geocode)
            tz = tf.timezone_at(lat=lat, lng=lon)
            if tz:
                conn.execute(
                    "UPDATE places SET timezone = ? WHERE id = ?",
                    (tz, place_id)
                )
            else:
                logger.warning(f"Could not determine timezone for place_id={place_id} ({lat}, {lon})")


@flow(name="Geocode Places")
def geocode_places_flow():
    rows = fetch_uncoded_places()

    if not rows:
        logger.info("No uncoded places found, skipping geocoding")
        return {"geocoded_count": 0}

    coords = [(lat, lon) for _, lat, lon in rows]
    geocodes = geocode_batch(coords)

    store_geocodes(rows, geocodes)

    return {"geocoded_count": len(rows)}