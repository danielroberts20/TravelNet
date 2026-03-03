import logging
import os
from fastapi import APIRouter, BackgroundTasks, Header  # type: ignore
from fastapi.responses import FileResponse  # type: ignore

from database.util import backup_db
from auth import check_auth


router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/download")
async def download(background_tasks: BackgroundTasks, authorization: str = Header(None)):
    check_auth(authorization)
    
    backup_path = backup_db()
    background_tasks.add_task(os.remove, backup_path)
    logger.info(f"Database backup created at {backup_path}, scheduled for deletion after response")

    return FileResponse(
        path=backup_path,
        filename=backup_path.name,
        media_type="application/octet-stream"
    )