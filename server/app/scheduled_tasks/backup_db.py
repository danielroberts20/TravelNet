"""
Weekly local DB backup with 4-week retention.

Scheduled: Every Sunday at 01:00.
  - Creates a timestamped .db snapshot in backups/db/
  - Deletes backups older than 28 days

Can also be triggered manually:
  docker exec travelnet python -m scheduled_tasks.backup_db
"""

from config.editable import load_overrides
load_overrides()

import logging
from database.util import backup_db
from config.general import DATABASE_BACKUP_DIR
from datetime import datetime, timedelta
from config.logging import configure_logging
from config.settings import settings
from notifications import CronJobMailer

logger = logging.getLogger(__name__)

def prune_old_backups(days: int = 28) -> int:
    """Delete backups older than `days` days. Returns count deleted."""
    cutoff = datetime.now() - timedelta(days=days)
    deleted = 0
    for f in DATABASE_BACKUP_DIR.glob("*.db"):
        try:
            mtime = datetime.fromtimestamp(f.stat().st_mtime)
            if mtime < cutoff:
                f.unlink()
                logger.info(f"Pruned old backup: {f.name}")
                deleted += 1
        except Exception as e:
            logger.warning(f"Failed to prune {f.name}: {e}")
    return deleted


def run():
    """Create a timestamped DB snapshot and prune backups older than 28 days."""
    backup_path = backup_db()
    size_mb     = round(backup_path.stat().st_size / (1024 * 1024), 2)
    deleted     = prune_old_backups(days=28)
    return {
        "backup_path": str(backup_path),
        "size_mb":     size_mb,
        "pruned":      deleted,
    }


if __name__ == "__main__":
    configure_logging()
    with CronJobMailer("backup_db", settings.smtp_config,
                       detail="Backup full DB to local storage, keep only last 4 weeks") as job:
        result = run()
        job.add_metric("backup", result["backup_path"])
        job.add_metric("size_mb", result["size_mb"])
        job.add_metric("pruned", result["pruned"])