import csv
import os
from fastapi import HTTPException  # type: ignore
import logging

from database.integration import init_db, insert_log
from telemetry_models import Log
from database.util import get_conn
from config.general import DATA_DIR, HEALTH_BACKUP_DIR


logger = logging.getLogger(__name__)

def rebuild_db():
    logger.info("Rebuilding database from CSV logs...")
    with get_conn() as conn:
        conn.execute("DROP TABLE IF EXISTS cellular_states;")
        conn.execute("DROP TABLE IF EXISTS locations;")
        conn.commit()   

    init_db()

    for file in sorted(os.listdir(HEALTH_BACKUP_DIR)):
        if file.endswith(".csv"):
            input_csv(open(HEALTH_BACKUP_DIR / file, "r"))
    logger.info("Finished rebuilding database")
            
def input_csv(csv_file):
    reader = csv.DictReader(csv_file)

    required_fields = {"latitude", "longitude", "timestamp"}

    if not required_fields.issubset(reader.fieldnames):
        raise HTTPException(
            status_code=400,
            detail=f"CSV must contain columns: {required_fields}"
        )

    inserted = 0
    skipped_rows = []
    total_rows = 0

    for idx, row in enumerate(reader):
        total_rows += 1
        try:
            log = Log.from_strings(**row)
            insert_log(log)
            inserted += 1
        except Exception as e:
            # Skip bad rows
            logger.warning(f"Bad row on line {idx+2}.\t CSV entry: {row}\tException: {e}")
            skipped_rows.append(idx+2)
            continue
    logger.info(f"Successfully uploaded {inserted}/{total_rows} entries")
    return inserted, skipped_rows