"""
Daily local DB backup with 4-week retention and upload backup pruning.
Scheduled: Daily at 01:00.
  - Creates a timestamped .db snapshot in backups/db/
  - Compresses snapshot with zstd (.db.zst)
  - Deletes .db.zst backups older than 28 days
  - Prunes upload backup directories per retention policy in config/pruning.py
"""
from config.editable import load_overrides
load_overrides()

import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from prefect import task, flow
from prefect.logging import get_run_logger

from database.connection import backup_db
from config.general import DATABASE_BACKUP_DIR, DB_FILE
from notifications import notify_on_completion, log_on_success, record_flow_result
from config.pruning import prune_directory, HEALTH_RETENTION_DAYS, LOCATION_RETENTION_DAYS


@task
def create_db_snapshot(prefix=None, suffix=None) -> dict:
    logger = get_run_logger()
    backup_path = backup_db(prefix, suffix)
    size_mb = round(backup_path.stat().st_size / (1024 * 1024), 2)
    logger.info("DB snapshot created: %s (%.2f MB)", backup_path, size_mb)
    return {"backup_path": str(backup_path), "size_mb": size_mb}


@task
def compress_snapshot(backup_path: str) -> dict:
    logger = get_run_logger()
    src = Path(backup_path)
    dest = src.with_suffix(src.suffix + ".zst")
    result = subprocess.run(
        ["zstd", str(src), "-o", str(dest)],
        capture_output=True, text=True, timeout=120,
    )
    if result.returncode != 0:
        raise RuntimeError(f"zstd compression failed: {result.stderr.strip()}")
    src.unlink()
    compressed_size_mb = round(dest.stat().st_size / (1024 * 1024), 2)
    logger.info("Compressed: %s (%.2f MB)", dest.name, compressed_size_mb)
    return {"compressed_path": str(dest), "compressed_size_mb": compressed_size_mb}


@task
def prune_old_db_backups(days: int = 28) -> int:
    logger = get_run_logger()
    cutoff = datetime.now() - timedelta(days=days)
    deleted = 0
    for f in DATABASE_BACKUP_DIR.glob("*.db.zst"):
        try:
            mtime = datetime.fromtimestamp(f.stat().st_mtime)
            if mtime < cutoff:
                f.unlink()
                logger.info("Pruned old DB backup: %s", f.name)
                deleted += 1
        except Exception as e:
            logger.warning("Failed to prune %s: %s", f.name, e)
    return deleted


@task
def prune_upload_backups() -> dict:
    """Prune upload backup directories according to retention policy in config/pruning.py."""
    logger = get_run_logger()
    from config.pruning import (
        prune_directory,
        HEALTH_RETENTION_DAYS,
        LOCATION_RETENTION_DAYS,
    )
    from config.general import (
        HEALTH_BACKUP_DIR,
        WORKOUT_BACKUP_DIR,
        LOCATION_OVERLAND_BACKUP_DIR,
        LOCATION_SHORTCUTS_BACKUP_DIR,
    )

    results = [
        prune_directory(HEALTH_BACKUP_DIR,             HEALTH_RETENTION_DAYS,   "health"),
        prune_directory(WORKOUT_BACKUP_DIR,            HEALTH_RETENTION_DAYS,   "workouts"),
        prune_directory(LOCATION_OVERLAND_BACKUP_DIR,  LOCATION_RETENTION_DAYS, "location_overland"),
        prune_directory(LOCATION_SHORTCUTS_BACKUP_DIR, LOCATION_RETENTION_DAYS, "location_shortcuts"),
        # Mood (Class E), FX (Class E), transactions (Class D), journal (Class C): not pruned
    ]

    for r in results:
        if r.files_deleted:
            logger.info(
                "Pruned %s: %d file(s), %.1f KB freed",
                r.domain, r.files_deleted, r.bytes_freed_kb,
            )

    total_deleted = sum(r.files_deleted for r in results)
    total_freed_kb = round(sum(r.bytes_freed_kb for r in results), 1)

    return {
        "domains": {
            r.domain: {
                "files_deleted": r.files_deleted,
                "bytes_freed_kb": r.bytes_freed_kb,
            }
            for r in results
        },
        "total_files_deleted": total_deleted,
        "total_kb_freed": total_freed_kb,
    }


@task
def analyze_db() -> None:
    """Run ANALYZE to refresh query planner statistics. Runs on the 1st of each month."""
    import sqlite3 as _sqlite3
    from datetime import datetime
    logger = get_run_logger()
    if datetime.now().day != 1:
        logger.debug("ANALYZE skipped (not 1st of month)")
        return
    logger.info("Running ANALYZE to refresh query planner statistics...")
    conn = _sqlite3.connect(str(DB_FILE), timeout=30)
    conn.execute("ANALYZE;")
    conn.close()
    logger.info("ANALYZE complete")


@flow(name="Backup DB", on_failure=[notify_on_completion], on_completion=[log_on_success])
def backup_db_flow(prefix: str | None = None, suffix: str | None = None):
    snapshot = create_db_snapshot(prefix, suffix)
    compressed = compress_snapshot(snapshot["backup_path"])
    pruned_db = prune_old_db_backups(28)
    pruned = prune_upload_backups()
    analyze_db()
    result = {
        "backup_path": compressed["compressed_path"],
        "snapshot_size_mb": snapshot["size_mb"],
        "compressed_size_mb": compressed["compressed_size_mb"],
        "db_files_pruned": pruned_db,
        "upload_files_pruned": pruned["total_files_deleted"],
        "upload_kb_freed": pruned["total_kb_freed"],
    }
    record_flow_result(result)
    return result

if __name__ == "__main__":
    backup_db_flow()