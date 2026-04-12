from config.editable import load_overrides
load_overrides()

from prefect import task, flow
from prefect.logging import get_run_logger

from config.general import LOCATION_NOISE_ACCURACY_THRESHOLD
from database.connection import get_conn
from database.location.noise.table import table as noise_table, LocationNoiseRecord


@task
def flag_tier1_noise() -> int:
    logger = get_run_logger()
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT o.id, o.horizontal_accuracy
            FROM location_overland o
            WHERE o.horizontal_accuracy > ?
            AND NOT EXISTS (
                SELECT 1 FROM location_noise n WHERE n.overland_id = o.id
            )
        """, (LOCATION_NOISE_ACCURACY_THRESHOLD,)).fetchall()

        for row in rows:
            noise_table.insert(LocationNoiseRecord(
                overland_id=row["id"],
                tier=1,
                reason="accuracy_threshold",
            ))

    logger.info("Flagged %d location point(s) as tier-1 noise", len(rows))
    return len(rows)


@flow(name="flag-location-noise")
def flag_location_noise_flow():
    flagged = flag_tier1_noise()
    return {"flagged": flagged}
