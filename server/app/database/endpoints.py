import logging
import os
from fastapi import APIRouter, BackgroundTasks, HTTPException, Depends  # type: ignore
from fastapi.responses import FileResponse  # type: ignore

from config.auth import require_upload_token
from database.util import backup_db, get_conn


router = APIRouter()
logger = logging.getLogger(__name__)

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
    "weather_daily",
    "known_places",
    "gap_annotations",
    "state_of_mind",
    "trigger_log",
]


@router.get("/download", dependencies=[Depends(require_upload_token)])
async def download(background_tasks: BackgroundTasks):
    """Create a point-in-time DB backup and stream it to the caller.

    The backup file is scheduled for deletion once the response is sent so it
    does not accumulate on disk.
    """
    backup_path = backup_db()
    background_tasks.add_task(os.remove, backup_path)
    logger.info(f"Database backup created at {backup_path}, scheduled for deletion after response")

    return FileResponse(
        path=backup_path,
        filename=backup_path.name,
        media_type="application/octet-stream"
    )


@router.get("/reset", dependencies=[Depends(require_upload_token)])
async def reset_table(table: str):
    """Delete all rows from a resettable table.

    Only tables explicitly listed in RESETTABLE_TABLES are permitted.
    Returns HTTP 400 for any other table name.
    """
    if table not in RESETTABLE_TABLES:
        raise HTTPException(status_code=400, detail=f"Table '{table}' is not resettable")
    with get_conn() as conn:
        conn.execute(f"DELETE FROM [{table}]")
        conn.commit()
        logger.warning(f"Table {table} was cleared!")
    return {"message": f"Table '{table}' cleared successfully"}
