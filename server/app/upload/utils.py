import csv
from fastapi import HTTPException  # type: ignore
import logging

from database.integration import insert_log
from telemetry_models import Log


logger = logging.getLogger(__name__)
            
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