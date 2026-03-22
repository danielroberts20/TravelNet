from datetime import datetime
import io
import logging

from fastapi import APIRouter, Header, Query, UploadFile, File, HTTPException, status, Depends, BackgroundTasks #type: ignore
from config.general import LOCATION_SHORTCUTS_BACKUP_DIR
from auth import check_auth, verify_token
from database.location.overland.table import insert_overland
from telemetry_models import OverlandPayload
from upload.utils import input_csv  

router = APIRouter()
logger = logging.getLogger(__name__)

@router.post("/shortcut")
async def upload_csv(file: UploadFile = File(...),
                     authorization: str = Header(None)):

    check_auth(authorization)

    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="File must be a CSV")

    contents = await file.read()

    # Decode bytes → string
    decoded = contents.decode("utf-8")
    now = datetime.now()
    with open(LOCATION_SHORTCUTS_BACKUP_DIR / f"{now.strftime('%Y-%m-%d')}.csv", "w") as f:
        f.write(decoded)

    # Convert string → file-like object
    csv_file = io.StringIO(decoded)
    
    inserted, skipped_rows = input_csv(csv_file)

    return {
        "status": "success",
        "rows_inserted": inserted,
        "skipped_rows": skipped_rows
    }

@router.post(
    "/overland",
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(verify_token)],
)
async def upload_overland(
        payload: OverlandPayload,
        background_tasks: BackgroundTasks,
        device_id: str = Query(default="unknown"),
):

    #insert_overland(payload)
    logger.info(f"Received Overland payload with {len(payload.locations)} entries.")
    background_tasks.add_task(insert_overland, payload, device_id)    
    return {"result": "ok"}

@router.post(
    "/discard",
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(verify_token)],
)
async def discard_overland():
    return {"result": "ok"}