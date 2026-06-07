"""
upload/health/pruning.py
~~~~~~~~~~~~~~~~~~~~~~~~
Thin compatibility wrapper. Pruning logic has moved to config/pruning.
This module is retained so any existing imports continue to work.
"""
from config.pruning import prune_directory, HEALTH_RETENTION_DAYS
from config.general import HEALTH_BACKUP_DIR, WORKOUT_BACKUP_DIR


def prune_health_backups(retention_days: int = HEALTH_RETENTION_DAYS) -> dict:
    """Backward-compatible wrapper. Prunes health and workout backup dirs."""
    r_health  = prune_directory(HEALTH_BACKUP_DIR,  retention_days, "health")
    r_workout = prune_directory(WORKOUT_BACKUP_DIR, retention_days, "workouts")
    return {
        "files_deleted": r_health.files_deleted + r_workout.files_deleted,
        "bytes_freed_kb": r_health.bytes_freed_kb + r_workout.bytes_freed_kb,
    }
