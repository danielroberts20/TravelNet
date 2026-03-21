from datetime import datetime
import json
import logging
from typing import Any

from auth import check_auth
from config.general import HEALTH_BACKUP_DIR
from fastapi import APIRouter, BackgroundTasks, Header  # type: ignore
from upload.health.util import handle_health_upload

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

    backup_path = HEALTH_BACKUP_DIR / f"{year_month}-{day}.json"
    with open(backup_path, "w+", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    health_data = data.get("data", {})
    metric_count = len(health_data.get("metrics", []))
    background_tasks.add_task(handle_health_upload, health_data)

    logger.info(f"Successfully received health upload with {metric_count} metrics.")
    return {
        "status": "success",
        "metrics_received": metric_count,
    }