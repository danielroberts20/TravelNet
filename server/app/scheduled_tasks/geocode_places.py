import logging

from config.logging import configure_logging
from database.connection import get_conn
from database.location.geocoding import batch_geocode, insert_geocode
from notifications import DailyCronJobMailer
from config.settings import settings


logger = logging.getLogger(__name__)

from timezonefinder import TimezoneFinder

def run():
    tf = TimezoneFinder()  # instantiate once, reuse for all rows
    
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT id, lat_snap, lon_snap FROM places WHERE
                geocoded_at IS NULL AND
                lat_snap IS NOT NULL AND
                lon_snap IS NOT NULL
        """).fetchall()
        logger.info(f"Found {len(rows)} places to geocode")

    coords = [(row[1], row[2]) for row in rows]
    ids = [row[0] for row in rows]
    geocodes = batch_geocode(coords)
    logger.info("Finished geocoding, inserting results")

    for place_id, (lat, lon), geocode in zip(ids, coords, geocodes):
        insert_geocode(place_id, geocode)
        tz = tf.timezone_at(lat=lat, lng=lon)  # returns e.g. "Europe/London" or None
        if tz:
            with get_conn() as conn:
                conn.execute(
                    "UPDATE places SET timezone = ? WHERE id = ?",
                    (tz, place_id)
                )
        else:
            logger.warning(f"Could not determine timezone for place_id={place_id} ({lat}, {lon})")

    return {"geocoded_count": len(coords)}


if __name__ == "__main__":
    configure_logging()

    with DailyCronJobMailer("geocode_places", settings.smtp_config,
                       detail="Reverse geocode place locations") as job:
        results = run()
        job.add_metric("geocoded count", results["geocoded_count"])