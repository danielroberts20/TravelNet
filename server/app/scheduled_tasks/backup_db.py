"""
Daily local DB backup with 4-week retention and health backup pruning.
Scheduled: Daily at 01:00.
  - Creates a timestamped .db snapshot in backups/db/
  - Deletes .db backups older than 28 days
  - Deletes health/workout/mood JSONL backups older than 14 days
"""
from config.editable import load_overrides
load_overrides()

from datetime import datetime, timedelta
from prefect import task, flow
from prefect.logging import get_run_logger
from database.connection import backup_db
from config.general import DATABASE_BACKUP_DIR
from notifications import notify_on_completion, log_on_success, record_flow_result
from upload.health.pruning import prune_health_backups


@task
def create_db_snapshot(prefix=None, suffix=None) -> dict:
    logger = get_run_logger()
    backup_path = backup_db(prefix, suffix)
    size_mb = round(backup_path.stat().st_size / (1024 * 1024), 2)
    logger.info("DB snapshot created: %s (%.2f MB)", backup_path, size_mb)
    return {"backup_path": str(backup_path), "size_mb": size_mb}


@task
def prune_old_db_backups(days: int = 28) -> int:
    logger = get_run_logger()
    cutoff = datetime.now() - timedelta(days=days)
    deleted = 0
    for f in DATABASE_BACKUP_DIR.glob("*.db"):
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
def prune_old_health_backups(days: int = 14) -> dict:
    logger = get_run_logger()
    result = prune_health_backups(retention_days=days)
    logger.info(
        "Health backup pruning complete: %d file(s) deleted, %.1f KB freed",
        result["files_deleted"],
        result["bytes_freed_kb"],
    )
    return result


@flow(name="Backup DB", on_failure=[notify_on_completion], on_completion=[log_on_success])
def backup_db_flow(prefix: str | None = None, suffix: str | None = None):
    snapshot = create_db_snapshot(prefix, suffix)
    pruned_db = prune_old_db_backups(28)
    pruned_health = prune_old_health_backups(14)
    result = {
        "backup_path": snapshot["backup_path"],
        "size_mb": snapshot["size_mb"],
        "db_files_pruned": pruned_db,
        "health_files_pruned": pruned_health["files_deleted"],
        "health_kb_freed": pruned_health["bytes_freed_kb"],
    }
    record_flow_result(result)
    return result