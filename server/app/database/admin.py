import logging
import os
from pathlib import Path
import shutil
import signal
import sqlite3
import subprocess
import tempfile
from typing import Generator, Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException, Depends, Query
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

from config.general import DB_FILE
from auth import require_upload_token
from database.connection import backup_db, get_conn
from database.pruning import (
    CASCADE_ONLY, DEFAULT_TABLES, TABLE_CONFIG,
    get_prune_counts, prune_before, validate_tables,
)
from config.settings import settings


router = APIRouter()
logger = logging.getLogger(__name__)

RESETTABLE_TABLES = [
    "country_transitions",
    "cost_of_living",
    "cron_results",
    "daily_summary",
    "flights",
    "fx_rates",
    "gap_annotations",
    "known_places",
    "location_noise",
    "log_digest",
    "ml_anomolies",
    "ml_location_cluster_members",
    "ml_location_clusters",
    "ml_segments",
    "photo_metadata",
    "place_visits",
    "places",
    "transition_timezone",
    "trigger_log",
    "watchdog_heartbeat",
    "weather_daily",
    "weather_hourly"
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

# ------------------------------------------------
# Restore from Cloudflare R2
# ------------------------------------------------

def _sse(message: str, level: str = "info") -> str:
    return f"data: {level}|{message}\n\n"


@router.get("/restore/list", dependencies=[Depends(require_upload_token)])
def list_r2_backups():
    """List .db.age backups available in Cloudflare R2."""
    try:
        result = subprocess.run(
            ["rclone", "ls", f"{settings.rclone_remote}:{settings.rclone_bucket}"],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode != 0:
            return {"error": result.stderr.strip(), "backups": []}

        backups = []
        for line in result.stdout.splitlines():
            parts = line.strip().split(None, 1)
            if len(parts) == 2 and parts[1].endswith(".db.age"):
                size_bytes, filename = parts
                backups.append({
                    "filename": filename.strip(),
                    "size_bytes": int(size_bytes),
                })

        return {
            "backups": sorted(backups, key=lambda x: x["filename"], reverse=True)
        }

    except FileNotFoundError:
        return {"error": "rclone not found in container", "backups": []}
    except subprocess.TimeoutExpired:
        return {"error": "rclone timed out listing R2", "backups": []}


def _restore_stream(filename: str, live: bool) -> Generator[str, None, None]:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        
        remote_path = Path(filename)          # can be "dir/name.db.age"
        local_name = remote_path.name         # always "name.db.age"

        encrypted = tmp_path / local_name
        decrypted = tmp_path / Path(local_name).with_suffix("")

        # Step 1: Download
        yield _sse(f"── Step 1: Downloading {filename} from R2…")
        r = subprocess.run(
            ["rclone", "copy",
             f"{settings.rclone_remote}:{settings.rclone_bucket}/{filename}",
             str(tmp_path)],
            capture_output=True, text=True, timeout=120
        )
        if r.returncode != 0:
            yield _sse(f"✗ Download failed: {r.stderr.strip()}", "error")
            return
        yield _sse("    ✓ Downloaded")

        # Step 2: Decrypt
        yield _sse("── Step 2: Decrypting…")
        r = subprocess.run(
            ["age", "--decrypt", "-i", settings.age_key_path,
             "-o", str(decrypted), str(encrypted)],
            capture_output=True, text=True, timeout=60
        )
        if r.returncode != 0:
            yield _sse(f"✗ Decryption failed: {r.stderr.strip()}", "error")
            return
        yield _sse("    ✓ Decrypted")

        # Step 3: Integrity check
        yield _sse("── Step 3: Integrity check…")
        try:
            conn = sqlite3.connect(str(decrypted))
            row = conn.execute("PRAGMA integrity_check;").fetchone()
            conn.close()
            if row[0] != "ok":
                yield _sse(f"✗ Integrity check FAILED: {row[0]}", "error")
                return
            yield _sse("    ✓ Integrity check passed")
        except Exception as e:
            yield _sse(f"✗ Integrity check error: {e}", "error")
            return

        # Step 4: Row counts
        yield _sse("── Step 4: Row counts…")
        try:
            conn = sqlite3.connect(str(decrypted))
            tables = [r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
            ).fetchall()]
            for table in tables:
                count = conn.execute(
                    f"SELECT COUNT(*) FROM [{table}];"
                ).fetchone()[0]
                yield _sse(f"    {table:<35} {count:>8,} rows")
            conn.close()
        except Exception as e:
            yield _sse(f"✗ Row count error: {e}", "error")
            return

        yield _sse("── Backup looks healthy.")
        yield _sse("")

        if not live:
            yield _sse(
                "ℹ  Dry run complete. No changes made to the live database.",
                "success"
            )
            return

        # Step 5: Replace live database
        yield _sse("── Step 5: Replacing live database…")
        try:
            # Checkpoint WAL before replacement so the copy is clean
            live_conn = sqlite3.connect(str(DB_FILE))
            live_conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
            live_conn.close()
            shutil.copy2(str(decrypted), str(DB_FILE))
            yield _sse("    ✓ Database replaced")
        except Exception as e:
            yield _sse(f"✗ Replace failed: {e}", "error")
            return

        yield _sse("── Step 6: Restarting ingest service…")
        yield _sse(
            "    Connection will drop. Wait ~1 minute then reload the Dashboard.",
            "success"
        )

        # SIGTERM triggers uvicorn graceful shutdown;
        # Docker restart policy brings the container back automatically.
        os.kill(os.getpid(), signal.SIGTERM)


@router.get("/restore/stream", dependencies=[Depends(require_upload_token)])
def stream_restore(
    filename: str = Query(...),
    live: bool = Query(False),
):
    """SSE stream for backup restore. Set live=true to replace the live database."""
    return StreamingResponse(
        _restore_stream(filename, live),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )