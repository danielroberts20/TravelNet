from datetime import datetime
import io
import logging

from fastapi import APIRouter, Query, UploadFile, File, HTTPException, status, Depends, BackgroundTasks  # type: ignore
from config.general import LOCATION_SHORTCUTS_BACKUP_DIR
from auth import require_upload_token, verify_overland_token
from database.location.overland.table import insert_overland
from telemetry_models import OverlandPayload
from upload.utils import input_csv
from upload.location.overland.backup import append_to_daily_buffer, log_previous_day_backup

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/shortcut", dependencies=[Depends(require_upload_token)])
async def upload_csv(
        file: UploadFile = File(...),
        background_tasks: BackgroundTasks = BackgroundTasks,
):
    """Accept a Shortcuts CSV location export, save a local backup, and queue processing.

    The file is written synchronously to LOCATION_SHORTCUTS_BACKUP_DIR so the
    backup is always present even if the background task fails.  Two background
    tasks are queued:
      - input_csv: parse and insert each row into the DB.
      - log_previous_day_backup: log a summary of yesterday's Overland JSONL file
        (this endpoint fires at ~02:56 UTC, by which point the previous day's
        Overland buffer is complete).
    """
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="File must be a CSV")

    contents = await file.read()

    # Decode bytes → string
    decoded = contents.decode("utf-8")
    now = datetime.now()
    with open(LOCATION_SHORTCUTS_BACKUP_DIR / f"{now.strftime('%Y-%m-%d')}.csv", "w") as f:
        f.write(decoded)

    # Convert string → file-like object and queue DB insert
    csv_file = io.StringIO(decoded)
    background_tasks.add_task(input_csv, csv_file)

    # Log yesterday's completed Overland buffer now that the day has rolled over
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
    """Accept an Overland GPS payload, append it to the daily JSONL buffer, and queue DB insert.

    Two background tasks are queued:
      - append_to_daily_buffer: write the raw payload to today's JSONL file.
      - insert_overland: normalise and upsert each location point into the DB.
    """
    logger.upload(f"Received Overland payload with {len(payload.locations)} entries.")
    background_tasks.add_task(append_to_daily_buffer, payload)
    background_tasks.add_task(insert_overland, payload, device_id)
    return {"result": "ok"}


@router.post(
    "/discard",
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(verify_overland_token)],
)
async def discard_overland():
    """Accept and silently discard an Overland payload (used for testing/muting)."""
    return {"result": "ok"}
