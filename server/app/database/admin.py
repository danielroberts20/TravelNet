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
    _CASCADE_PARENTS,
    get_prune_counts, prune_before, validate_tables,
)
from config.settings import settings


router = APIRouter()
logger = logging.getLogger(__name__)

# Tables excluded from the Dashboard reset endpoint (full table wipe).
# Separate from EXCLUDE_TABLES in pruning.py — that controls timestamp-based
# pruning; this controls whether a table can be fully cleared in one operation.
RESET_EXCLUDE: set[str] = {
    "places",            # geographic reference grid; everything FKs into this
    "fx_rates",          # needed for backfill_gbp on surviving transactions
    "api_usage",         # monthly counter; wiping mid-month breaks quota tracking
    "cost_of_living",    # manually curated reference data; no re-ingestion path
    "flights",           # manually logged; no automated re-ingestion path
    "known_places",      # spatial memory built over weeks; not casually resettable
    "place_visits",      # visits linked to known_places; same reasoning
    "transition_timezone",     # structural; re-seeded by dedicated flow only
    "country_transitions",     # structural; re-seeded by dedicated flow only
    "ml_day_embeddings",       # expensive to regenerate; cleared by model rerun only
    "ml_destination_profiles",
    "ml_causal_graph",
}

RESETTABLE_TABLES: list[str] = sorted(
    set(TABLE_CONFIG.keys()) - RESET_EXCLUDE
)


class PruneRequest(BaseModel):
    cutoff: str
    tables: Optional[list[str]] = None

@router.get("/resettable-tables", dependencies=[Depends(require_upload_token)])
async def get_resettable_tables():
    """Return the tables eligible for resetting."""
    return {"tables": RESETTABLE_TABLES}

@router.get("/prune/tables", dependencies=[Depends(require_upload_token)])
async def prune_tables_list():
    """Return the tables eligible for pruning and their cascade relationships."""
    return {
        "tables":          list(TABLE_CONFIG.keys()),
        "cascade_only":    list(CASCADE_ONLY),
        "pre_delete":      [],
        "cascade_parents": _CASCADE_PARENTS,
        "default":         DEFAULT_TABLES,
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


@router.post("/truncate-resettable", dependencies=[Depends(require_upload_token)])
async def truncate_resettable():
    """Delete all rows from every resettable table.

    Clears each table in FK-safe order (children before parents) so that
    FK constraints are not violated even when foreign_keys is ON.
    Returns the list of tables that were cleared.
    """
    # Use DELETION_ORDER so children are deleted before their parents,
    # then append cascade-only tables (they cascade automatically but we
    # also clear them explicitly so counts go to zero reliably).
    ordered = [t for t in DEFAULT_TABLES if t in set(RESETTABLE_TABLES)]
    # Any resettable tables not in DEFAULT_TABLES come last.
    remaining = [t for t in RESETTABLE_TABLES if t not in set(ordered)]
    to_clear = ordered + remaining

    cleared: list[str] = []
    with get_conn() as conn:
        for table in to_clear:
            conn.execute(f"DELETE FROM [{table}]")
            cleared.append(table)
        conn.commit()

    logger.warning("Truncate-all executed: cleared %d tables: %s", len(cleared), cleared)
    return {"cleared": cleared, "count": len(cleared)}

# ------------------------------------------------
# Restore from Cloudflare R2
# ------------------------------------------------

def _sse(message: str, level: str = "info") -> str:
    return f"data: {level}|{message}\n\n"


@router.get("/restore/list", dependencies=[Depends(require_upload_token)])
def list_r2_backups():
    """List .db.age and .db.zst.age backups available in Cloudflare R2."""
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
            if len(parts) == 2 and parts[1].endswith(".age"):
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
    with tempfile.TemporaryDirectory(dir="/data") as tmp:
        tmp_path = Path(tmp)
        
        remote_path = Path(filename)          # can be "dir/name.db.age"
        local_name = remote_path.name         # always "name.db.age"

        encrypted = tmp_path / local_name
        after_decrypt = tmp_path / Path(local_name).with_suffix("")   # strips .age
        is_compressed = after_decrypt.suffix == ".zst"
        db_path = after_decrypt.with_suffix("") if is_compressed else after_decrypt

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
             "-o", str(after_decrypt), str(encrypted)],
            capture_output=True, text=True, timeout=600
        )
        if r.returncode != 0:
            yield _sse(f"✗ Decryption failed: {r.stderr.strip()}", "error")
            return
        yield _sse("    ✓ Decrypted")

        # Step 3: Decompress
        yield _sse("── Step 3: Decompressing…")
        if is_compressed:
            r = subprocess.run(
                ["zstd", "--decompress", str(after_decrypt), "-o", str(db_path)],
                capture_output=True, text=True, timeout=120
            )
            if r.returncode != 0:
                yield _sse(f"✗ Decompression failed: {r.stderr.strip()}", "error")
                return
            yield _sse("    ✓ Decompressed")
        else:
            yield _sse("    ↷ Skipped (not a .zst file)")

        # Step 4: Integrity check
        yield _sse("── Step 4: Integrity check…")
        try:
            conn = sqlite3.connect(str(db_path))
            row = conn.execute("PRAGMA integrity_check;").fetchone()
            conn.close()
            if row[0] != "ok":
                yield _sse(f"✗ Integrity check FAILED: {row[0]}", "error")
                return
            yield _sse("    ✓ Integrity check passed")
        except Exception as e:
            yield _sse(f"✗ Integrity check error: {e}", "error")
            return

        # Step 5: Row counts
        yield _sse("── Step 5: Row counts…")
        try:
            conn = sqlite3.connect(str(db_path))
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

        # Step 6: Replace live database
        yield _sse("── Step 6: Replacing live database…")
        try:
            # Checkpoint WAL before replacement so the copy is clean
            live_conn = sqlite3.connect(str(DB_FILE))
            live_conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
            live_conn.close()
            shutil.copy2(str(db_path), str(DB_FILE))
            yield _sse("    ✓ Database replaced")
        except Exception as e:
            yield _sse(f"✗ Replace failed: {e}", "error")
            return

        yield _sse("── Step 7: Restarting ingest service…")
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