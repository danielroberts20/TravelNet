from datetime import datetime
import logging
from typing import Any

from auth import check_auth
from config.general import HEALTH_BACKUP_DIR
from fastapi import APIRouter, BackgroundTasks, Header #type: ignore
from upload.health.util import handle_health_upload 

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