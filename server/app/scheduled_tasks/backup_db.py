"""
Weekly local DB backup with 4-week retention.

Scheduled: Every Sunday at 01:00.
  - Creates a timestamped .db snapshot in backups/db/
  - Deletes backups older than 28 days
"""
from config.editable import load_overrides
load_overrides()

from datetime import datetime, timedelta

from prefect import task, flow
from prefect.logging import get_run_logger

from database.connection import backup_db
from config.general import DATABASE_BACKUP_DIR
from notifications import notify_on_completion, record_flow_result



@task
def create_db_snapshot(prefix=None, suffix=None) -> dict:
    logger = get_run_logger()
    backup_path = backup_db(prefix, suffix)
    size_mb = round(backup_path.stat().st_size / (1024 * 1024), 2)
    logger.info("DB snapshot created: %s (%.2f MB)", backup_path, size_mb)
    return {"backup_path": str(backup_path), "size_mb": size_mb}


@task
def prune_old_db_backups(days: int = 10) -> int:
    logger = get_run_logger()
    cutoff = datetime.now() - timedelta(days=days)
    deleted = 0
    for f in DATABASE_BACKUP_DIR.glob("*.db"):
        try:
            mtime = datetime.fromtimestamp(f.stat().st_mtime)
            if mtime < cutoff:
                f.unlink()
                logger.info("Pruned old backup: %s", f.name)
                deleted += 1
        except Exception as e:
            logger.warning("Failed to prune %s: %s", f.name, e)
    return deleted


@flow(name="Backup DB", on_failure=[notify_on_completion])
def backup_db_flow(prefix: str | None = None, suffix: str | None = None):
    snapshot = create_db_snapshot(prefix, suffix)
    pruned = prune_old_db_backups(10)
    result = {
        "backup_path": snapshot["backup_path"],
        "size_mb": snapshot["size_mb"],
        "pruned": pruned,
    }
    record_flow_result(result)
    return result
