import csv
from datetime import datetime
import io
from fastapi import APIRouter, Header, BackgroundTasks, UploadFile, File, HTTPException  #type: ignore
import logging
from typing import Any

from auth import check_auth
from config.general import HEALTH_BACKUP_DIR, LOCATION_BACKUP_DIR, REVOLUT_TRANSACTION_BACKUP_DIR, WISE_TRANSACTION_BACKUP_DIR
from database.transaction.ingest.wise import insert as insert_wise
from database.transaction.ingest.revolut import insert as insert_revolut
from uploads.utils import handle_health_upload, input_csv

router = APIRouter()
logger = logging.getLogger(__name__)

@router.post("/health")
async def upload_health(data: dict[str, Any],
                        background_tasks: BackgroundTasks,
                        authorization: str = Header(...)):

    check_auth(authorization)

    now = datetime.now()
    year_month = now.strftime("%Y-%m")
    day = int(now.strftime("%d"))-1
    with open(HEALTH_BACKUP_DIR / f"{year_month}-{day}.json", "w+") as f:
        f.write(str(data))
        f.close()

    background_tasks.add_task(handle_health_upload, data)    
    logger.info(f"Successfully uploaded {len(data)} health entries")
    return {
        "status": "success",
        "csvs_received": len(data)
    }

@router.post("/csv")
async def upload_csv(file: UploadFile = File(...),
                     authorization: str = Header(None)):

    check_auth(authorization)

    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="File must be a CSV")

    contents = await file.read()

    # Decode bytes → string
    decoded = contents.decode("utf-8")
    now = datetime.now()
    year_month = now.strftime("%Y-%m")
    day = int(now.strftime("%d"))-1
    with open(LOCATION_BACKUP_DIR / f"{year_month}-{day}.csv", "w+") as f:
        f.write(decoded)
        f.close()

    # Convert string → file-like object
    csv_file = io.StringIO(decoded)
    
    inserted, skipped_rows = input_csv(csv_file)

    return {
        "status": "success",
        "rows_inserted": inserted,
        "skipped_rows": skipped_rows
    }

@router.post("/wise")
async def upload_wise(file: UploadFile = File(...),
                      authorization: str = Header(None),
                      source: str = Header(None)):
    check_auth(authorization)

    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="File must be a CSV")

    print(source)
    
    contents = await file.read()

    # Decode bytes → string
    decoded = contents.decode("utf-8")
    now = datetime.now()
    year_month = now.strftime("%Y-%m")
    day = int(now.strftime("%d"))-1
    csv_path = WISE_TRANSACTION_BACKUP_DIR / f"{year_month}-{day}.csv"
    with open(csv_path, "w+") as f:
        f.write(decoded)
        f.close()

    inserted, skipped, errors = insert_wise(csv_path, source)

    return {
        "inserted": inserted,
        "skipped": skipped,
        "errors": errors
    }

@router.post("/revolut")
async def upload_revolut(file: UploadFile = File(...),
                      authorization: str = Header(None)):
    check_auth(authorization)

    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="File must be a CSV")
    
    contents = await file.read()

    # Decode bytes → string
    decoded = contents.decode("utf-8")
    now = datetime.now()
    year_month = now.strftime("%Y-%m")
    day = int(now.strftime("%d"))-1
    csv_path = REVOLUT_TRANSACTION_BACKUP_DIR / f"{year_month}-{day}.csv"
    with open(csv_path, "w+") as f:
        f.write(decoded)
        f.close()

    inserted, skipped, errors = insert_revolut(csv_path)

    return {
        "inserted": inserted,
        "skipped": skipped,
        "errors": errors
    }