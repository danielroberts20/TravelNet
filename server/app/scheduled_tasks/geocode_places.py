from config.editable import load_overrides
load_overrides()

import logging
from timezonefinder import TimezoneFinder

from prefect import get_run_logger, task, flow

from config.settings import settings
from database.connection import get_conn
from database.location.geocoding import batch_geocode, insert_geocode
from notifications import record_flow_result
from itertools import batched


@task
def fetch_uncoded_places() -> list[tuple[int, float, float]]:
    logger = get_run_logger()
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT id, lat_snap, lon_snap FROM places
            WHERE (geocoded_at IS NULL OR raw_json IS NULL)
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

    logger = get_run_logger()

    tf = TimezoneFinder()
    with get_conn() as conn:
        for (place_id, lat, lon), geocode in zip(rows, geocodes):
            try:
                insert_geocode(place_id, geocode, conn)
            except Exception as e:
                logger.error(str(e))
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
    logger = get_run_logger()
    rows = fetch_uncoded_places()

    if not rows:
        logger.info("No uncoded places found, skipping geocoding")
        result = {"geocoded_count": 0}
        record_flow_result(result)
        return result

    for i, batch in enumerate(batched(rows, n=50)):          # batch rows, not coords
        batch = list(batch)
        coords = [(lat, lon) for _, lat, lon in batch]       # derive coords from batch
        logger.info(f"Batch {i+1}: {len(batch)} places")
        geocodes, errors = geocode_batch(coords)
        if errors:
            for e in errors:
                logger.error(f"Error processing {e['lat']}, {e['lon']}: {e['error']}")
        store_geocodes(batch, geocodes)                       # only current batch's rows

    result = {"geocoded_count": len(rows)}
    record_flow_result(result)
    return result