from datetime import datetime
import io
import logging

from fastapi import APIRouter, Query, UploadFile, File, HTTPException, status, Depends, BackgroundTasks  # type: ignore
from config.general import LOCATION_SHORTCUTS_BACKUP_DIR
from auth import require_upload_token, verify_overland_token
from database.location.overland.table import table as overland_table
from models.telemetry import OverlandPayload
from upload.location.shortcuts import input_csv
from upload.location.overland.backup import append_to_daily_buffer, log_previous_day_backup
from triggers import location_change

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/shortcut", dependencies=[Depends(require_upload_token)])
async def upload_csv(
        file: UploadFile = File(...),
        background_tasks: BackgroundTasks = BackgroundTasks,
):
    """Accept a Shortcuts CSV location export, save a local backup, and queue processing."""
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="File must be a CSV")

    contents = await file.read()

    decoded = contents.decode("utf-8")
    now = datetime.now()
    backup_path = LOCATION_SHORTCUTS_BACKUP_DIR / f"{now.strftime('%Y-%m-%d')}.csv"
    file_exists = backup_path.exists()
    with open(backup_path, "a") as f:
        if file_exists:
            # Strip header row on subsequent appends to keep a single header per daily file
            lines = decoded.splitlines(keepends=True)
            f.writelines(lines[1:])
        else:
            f.write(decoded)

    csv_file = io.StringIO(decoded)
    background_tasks.add_task(input_csv, csv_file)
    background_tasks.add_task(log_previous_day_backup)

    return {
        "status": "success"
    }


@router.post(
    "/overland",
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(verify_overland_token)],
)
async def upload_overland(
        payload: OverlandPayload,
        background_tasks: BackgroundTasks,
        device_id: str = Query(default="unknown"),
):
    """Accept an Overland GPS payload, append it to the daily JSONL buffer, and queue DB insert."""
    logger.info(f"Received Overland payload with {len(payload.locations)} entries.")
    background_tasks.add_task(append_to_daily_buffer, payload)
    background_tasks.add_task(overland_table.insert_payload, payload, device_id)
    background_tasks.add_task(location_change.run)
    return {"result": "ok"}


@router.post(
    "/discard",
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(verify_overland_token)],
)
async def discard_overland():
    """Accept and silently discard an Overland payload (used for testing/muting)."""
    return {"result": "ok"}
