from datetime import datetime
import json
import logging
from typing import Any

from upload.health.workout_util import handle_workout_upload
from auth import check_auth
from config.general import HEALTH_BACKUP_DIR, WORKOUT_BACKUP_DIR
from fastapi import APIRouter, BackgroundTasks, Header, HTTPException  # type: ignore
from upload.health.health_util import handle_health_upload

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/data")
async def upload_health(
    data: dict[str, Any],
    background_tasks: BackgroundTasks,
    authorization: str = Header(...),
):
    check_auth(authorization)

    now = datetime.now()
    year_month = now.strftime("%Y-%m")
    day = int(now.strftime("%d")) - 1

    backup_path = HEALTH_BACKUP_DIR / f"{now.strftime('%Y-%m-%d_%H-%M-%S')}.json"
    with open(backup_path, "w+", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    health_data = data.get("data", {})
    metric_count = len(health_data.get("metrics", []))

    if metric_count == 0:
        logger.warning("Received empty health payload - returning 422 to prevent sync window advancing.")
        raise HTTPException(status_code=422, detail="Empty payload")
    
    background_tasks.add_task(handle_health_upload, health_data)

    logger.info(f"Successfully received health upload with {metric_count} metrics.")
    return {
        "status": "success",
        "metrics_received": metric_count,
    }

@router.post("/workout")
async def upload_workout(
    data: dict[str, Any],
    background_tasks: BackgroundTasks,
    authorization: str = Header(...),
):
    check_auth(authorization)
 
    now = datetime.now()
    year_month = now.strftime("%Y-%m")
    day = int(now.strftime("%d")) - 1
 
    backup_path = WORKOUT_BACKUP_DIR / f"{now.strftime('%Y-%m-%d_%H-%M-%S')}.json"
    with open(backup_path, "w+", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    workout_data = data.get("data", {})
    workout_count = len(workout_data.get("workouts", []))

    if workout_count == 0:
        logger.warning("Received empty workout payload - returning 422 to prevent sync window advancing.")
        raise HTTPException(status_code=422, detail="Empty payload")
    
    background_tasks.add_task(handle_workout_upload, workout_data)
 
    logger.info("Successfully received workout upload with %d workouts.", workout_count)
    return {
        "status": "success",
        "workouts_received": workout_count,
    }