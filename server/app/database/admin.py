import logging
import os
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException, Depends
from fastapi.responses import FileResponse
from pydantic import BaseModel

from auth import require_upload_token
from database.connection import backup_db, get_conn
from database.pruning import (
    CASCADE_ONLY, DEFAULT_TABLES, TABLE_CONFIG,
    get_prune_counts, prune_before, validate_tables,
)


router = APIRouter()
logger = logging.getLogger(__name__)

RESETTABLE_TABLES = [
    "transactions",
    "fx_rates",
    "api_usage",
    "log_digest",
    "health_quantity",
    "health_heart_rate",
    "health_sleep",
    "workouts",
    "workout_route",
    "cellular_state",
    "weather_hourly",
    "weather_daily",
    "places",
    "known_places",
    "gap_annotations",
    "state_of_mind",
    "mood_labels",
    "mood_associations",
    "trigger_log",
    "ml_location_clusters",
    "ml_location_cluster_members",
    "ml_segments",
    "ml_anomalies",
]


class PruneRequest(BaseModel):
    cutoff: str
    tables: Optional[list[str]] = None


@router.get("/prune/tables", dependencies=[Depends(require_upload_token)])
async def prune_tables_list():
    """Return the tables eligible for pruning and their cascade relationships."""
    return {
        "tables":      list(TABLE_CONFIG.keys()),
        "cascade_only": list(CASCADE_ONLY),
        "pre_delete":  [],  # no pre-delete tables — all have timestamps or cascade
        "cascade_parents": {
            # True SQLite ON DELETE CASCADE relationships
            "mood_labels":                 "state_of_mind",
            "mood_associations":           "state_of_mind",
            "location_noise":              "location_overland",
            "cellular_state":              "location_shortcuts",
            "workout_route":               "workouts",
            "place_visits":                "known_places",
            # ml_location_cluster_members cascades from location_overland (overland_id FK)
            # but is deleted directly via its own created_at — not listed here as cascade
        },
        "default": DEFAULT_TABLES,
    }


@router.post("/prune/preview", dependencies=[Depends(require_upload_token)])
async def prune_preview(req: PruneRequest):
    """Return row counts that would be deleted — no data is modified."""
    try:
        with get_conn(read_only=True) as conn:
            counts = get_prune_counts(conn, req.cutoff, req.tables)
        return {"counts": counts}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/prune/execute", dependencies=[Depends(require_upload_token)])
async def prune_execute(req: PruneRequest):
    """Create a pre-prune backup then delete rows older than cutoff."""
    try:
        validate_tables(req.tables or DEFAULT_TABLES)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    try:
        backup_path = backup_db(prefix="pre_prune")
        with get_conn() as conn:
            deleted = prune_before(conn, req.cutoff, req.tables)
        logger.warning(
            f"Prune executed: cutoff={req.cutoff}, tables={req.tables}, "
            f"deleted={deleted}, backup={backup_path}"
        )
        return {"deleted": deleted, "backup": str(backup_path)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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
        media_type="application/octet-stream",
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