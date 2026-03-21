import logging
import os
from fastapi import APIRouter, BackgroundTasks, Header, HTTPException  # type: ignore
from fastapi.responses import FileResponse  # type: ignore

from database.util import backup_db, get_conn
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

@router.get("/reset")
async def reset_table(table: str, authorization: str = Header(None)):
    check_auth(authorization)
    RESETTABLE_TABLES = [
        "transactions",
        "fx_rates",
        "api_usage",
        "log_digest",
        "health_data",
        "health_sources",
        "workouts",
        "workout_route",
        "jobs",
        "cellular_state",
        "weather_hourly",
        "weather_daily"
    ]
    if table not in RESETTABLE_TABLES:
        raise HTTPException(status_code=400, detail=f"Table '{table}' is not resettable")
    with get_conn() as conn:
        conn.execute(f"DELETE FROM [{table}]")
        conn.commit()
        logger.warning(f"Table {table} was cleared!")
    return {"message": f"Table '{table}' cleared successfully"}