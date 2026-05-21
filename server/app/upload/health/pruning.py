from datetime import datetime, timedelta
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


def _prune_dir(directory: Path, retention_days: int) -> tuple[int, int]:
    """Delete JSONL files older than retention_days. Returns (files_deleted, bytes_freed)."""
    cutoff = datetime.now() - timedelta(days=retention_days)
    files_deleted = 0
    bytes_freed = 0
    for path in directory.glob("*.jsonl"):
        try:
            file_date = datetime.strptime(path.stem, "%Y-%m-%d")
        except ValueError:
            continue  # skip anything not matching YYYY-MM-DD.jsonl
        if file_date < cutoff:
            bytes_freed += path.stat().st_size
            path.unlink()
            files_deleted += 1
    return files_deleted, bytes_freed


def prune_health_backups(retention_days: int = 14) -> dict:
    """Prune health, workout, and mood JSONL backup files older than retention_days.

    Returns a summary dict suitable for inclusion in cron_results.
    """
    from config.general import HEALTH_BACKUP_DIR, WORKOUT_BACKUP_DIR, MOOD_BACKUP_DIR

    total_files = 0
    total_bytes = 0
    detail = {}

    for name, directory in [
        ("health", HEALTH_BACKUP_DIR),
        ("workout", WORKOUT_BACKUP_DIR),
        ("mood", MOOD_BACKUP_DIR),
    ]:
        files, bytes_freed = _prune_dir(directory, retention_days)
        detail[name] = {"files_deleted": files, "bytes_freed": bytes_freed}
        total_files += files
        total_bytes += bytes_freed
        if files:
            logger.info(
                f"Pruned {files} {name} backup file(s) "
                f"({bytes_freed / 1024:.1f} KB freed)"
            )

    if total_files == 0:
        logger.info("Health backup pruning: nothing to prune.")

    return {
        "files_deleted": total_files,
        "bytes_freed_kb": round(total_bytes / 1024, 1),
        "detail": detail,
    }