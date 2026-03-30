from datetime import datetime
import json
import logging
from typing import Any

from database.health.mood.table import insert_state_of_mind
from upload.health.workout_util import handle_workout_upload
from config.auth import require_upload_token
from config.general import HEALTH_BACKUP_DIR, WORKOUT_BACKUP_DIR
from fastapi import APIRouter, BackgroundTasks, HTTPException, Depends  # type: ignore
from upload.health.health_util import handle_health_upload

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/data", dependencies=[Depends(require_upload_token)])
async def upload_health(
    data: dict[str, Any],
    background_tasks: BackgroundTasks,
):
    """Accept a Health Auto Export JSON payload, save a local backup, and queue processing.

    Returns HTTP 422 if the payload contains no metrics — this prevents the
    HAE sync window from advancing when an empty upload is received.
    """
    now = datetime.now()

    backup_path = HEALTH_BACKUP_DIR / f"{now.strftime('%Y-%m-%d_%H-%M-%S')}.json"
    with open(backup_path, "w+", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    health_data = data.get("data", {})
    metric_count = len(health_data.get("metrics", []))

    if metric_count == 0:
        logger.warning("Received empty health payload - returning 422 to prevent sync window advancing.")
        raise HTTPException(status_code=422, detail="Empty payload")

    background_tasks.add_task(handle_health_upload, health_data)

    logger.upload(f"Successfully received health upload with {metric_count} metrics.")
    return {
        "status": "success",
        "metrics_received": metric_count,
    }


@router.post("/workout", dependencies=[Depends(require_upload_token)])
async def upload_workout(
    data: dict[str, Any],
    background_tasks: BackgroundTasks,
):
    """Accept a Health Auto Export workout JSON payload, save a local backup, and queue processing.

    Returns HTTP 422 if the payload contains no workouts — this prevents the
    HAE sync window from advancing when an empty upload is received.
    """
    now = datetime.now()

    backup_path = WORKOUT_BACKUP_DIR / f"{now.strftime('%Y-%m-%d_%H-%M-%S')}.json"
    with open(backup_path, "w+", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    workout_data = data.get("data", {})
    workout_count = len(workout_data.get("workouts", []))

    if workout_count == 0:
        logger.warning("Received empty workout payload - returning 422 to prevent sync window advancing.")
        raise HTTPException(status_code=422, detail="Empty payload")

    background_tasks.add_task(handle_workout_upload, workout_data)

    logger.upload(f"Successfully received workout upload with {workout_count} workouts.")
    return {
        "status": "success",
        "workouts_received": workout_count,
    }

@router.post("/mood", dependencies=[Depends(require_upload_token)])
async def upload_mood(
    data: dict[str, Any],
    background_tasks: BackgroundTasks,
):
    entries = data.get("data", {}).get("stateOfMind", [])
    if not entries:
        raise HTTPException(status_code=422, detail="No stateOfMind entries found")

    background_tasks.add_task(insert_state_of_mind, entries)
    return {"status": "success", "queued": len(entries)}

