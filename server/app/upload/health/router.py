from datetime import datetime
import json
import logging
from typing import Any

from database.health.mood.table import table as mood_table
from upload.health.workouts import handle_workout_upload
from auth import require_upload_token
from config.general import HEALTH_BACKUP_DIR, WORKOUT_BACKUP_DIR, MOOD_BACKUP_DIR
from fastapi import APIRouter, BackgroundTasks, HTTPException, Depends
from upload.health.processing import handle_health_upload

router = APIRouter()
logger = logging.getLogger(__name__)


def _append_backup(directory, data: dict) -> None:
    """Append a JSON payload as a single line to today's JSONL backup file."""
    path = directory / f"{datetime.now().strftime('%Y-%m-%d')}.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)  # <-- add this
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(data, ensure_ascii=False) + "\n")


@router.post("/data", dependencies=[Depends(require_upload_token)])
async def upload_health(
    data: dict[str, Any],
    background_tasks: BackgroundTasks,
):
    """Accept a Health Auto Export JSON payload, save a local backup, and queue processing.

    Returns HTTP 422 if the payload contains no metrics — this prevents the
    HAE sync window from advancing when an empty upload is received.
    """
    _append_backup(HEALTH_BACKUP_DIR, data)

    health_data = data.get("data", {})
    metric_count = len(health_data.get("metrics", []))

    if metric_count == 0:
        logger.warning("Received empty health payload - returning 422 to prevent sync window advancing.")
        raise HTTPException(status_code=422, detail="Empty payload")

    background_tasks.add_task(handle_health_upload, health_data)
    logger.info(f"Successfully received health upload with {metric_count} metrics.")
    return {"status": "success", "metrics_received": metric_count}


@router.post("/workout", dependencies=[Depends(require_upload_token)])
async def upload_workout(
    data: dict[str, Any],
    background_tasks: BackgroundTasks,
):
    """Accept a Health Auto Export workout JSON payload, save a local backup, and queue processing.

    Returns HTTP 422 if the payload contains no workouts — this prevents the
    HAE sync window from advancing when an empty upload is received.
    """
    _append_backup(WORKOUT_BACKUP_DIR, data)

    workout_data = data.get("data", {})
    workout_count = len(workout_data.get("workouts", []))

    if workout_count == 0:
        logger.warning("Received empty workout payload - returning 422 to prevent sync window advancing.")
        raise HTTPException(status_code=422, detail="Empty payload")

    background_tasks.add_task(handle_workout_upload, workout_data)
    logger.info(f"Successfully received workout upload with {workout_count} workouts.")
    return {"status": "success", "workouts_received": workout_count}


@router.post("/mood", dependencies=[Depends(require_upload_token)])
async def upload_mood(
    data: dict[str, Any],
    background_tasks: BackgroundTasks,
):
    _append_backup(MOOD_BACKUP_DIR, data)

    entries = data.get("data", {}).get("stateOfMind", [])
    if not entries:
        raise HTTPException(status_code=422, detail="No stateOfMind entries found")

    background_tasks.add_task(mood_table.batch_insert, entries)
    return {"status": "success", "queued": len(entries)}