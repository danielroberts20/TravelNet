import logging

from config.logging import configure_logging
from database.connection import get_conn
from database.location.util import batch_geocode, insert_geocode
from notifications import CronJobMailer
from config.settings import settings


logger = logging.getLogger(__name__)

def run():
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
    for place_id, geocode in zip(ids, geocodes):
        insert_geocode(place_id, geocode)
    return {
        "geocoded_count": len(coords),
    }


if __name__ == "__main__":
    configure_logging()

    with CronJobMailer("geocode_places", settings.smtp_config,
                       detail="Reverse geocode place locations") as job:
        results = run()
        job.add_metric("geocoded count", results["geocoded_count"])