"""
metadata/backups.py
~~~~~~~~~~~~~~~~~~~~
Backup inspection helpers for the metadata dashboard endpoint.

Provides:
  - get_local_backups()  — info dicts for the most recent local backup per type
  - get_remote_backups() — info dict for the most recent Cloudflare R2 backup
"""

from datetime import datetime, timezone
import json
import subprocess

from config.general import (
    DATABASE_BACKUP_DIR, FX_BACKUP_DIR, HEALTH_BACKUP_DIR,
    LOCATION_OVERLAND_BACKUP_DIR, LOCATION_SHORTCUTS_BACKUP_DIR,
    REVOLUT_BACKUP_DIR, STALE_DAYS, WISE_BACKUP_DIR, WORKOUT_BACKUP_DIR,
)


def _backup_info(path) -> dict | None:
    """Return info dict for a single backup file, or None if it doesn't exist."""
    try:
        stat = path.stat()
        return {
            "filename":    path.name,
            "size_mb":     round(stat.st_size / (1024 * 1024), 2),
            "modified_ts": int(stat.st_mtime),
            "modified":    datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
            "stale":       (datetime.now(tz=timezone.utc).timestamp() - stat.st_mtime) > (STALE_DAYS * 86400),
        }
    except Exception:
        return None


def _latest_in_dir(directory, pattern="*", index: int = 0) -> dict | None:
    """Return backup info for the most recently modified file matching pattern.

    index: Which file to return. Default is most recent file (0).
           For the nth most recent file, use index n-1.
    """
    try:
        files = sorted(directory.glob(pattern), key=lambda f: f.stat().st_mtime, reverse=True)
        if not files:
            return None
        try:
            info = _backup_info(files[index])
        except IndexError:
            info = _backup_info(files[-1])  # fall back to oldest if index out of range
        if info:
            info["count"] = len(files)
        return info
    except Exception as e:
        return {"error": str(e)}


def get_local_backups() -> dict:
    """Return latest backup info for each local backup type."""
    return {
        "db":       _latest_in_dir(DATABASE_BACKUP_DIR, "*.db"),
        "fx":       _latest_in_dir(FX_BACKUP_DIR, "*.json"),
        "health":   _latest_in_dir(HEALTH_BACKUP_DIR, "*.json"),
        "workouts": _latest_in_dir(WORKOUT_BACKUP_DIR, "*.json"),
        "location": {
            "shortcut": _latest_in_dir(LOCATION_SHORTCUTS_BACKUP_DIR, "*.csv"),
            "overland": _latest_in_dir(LOCATION_OVERLAND_BACKUP_DIR, "*.jsonl", index=1),
        },
        "revolut":  _latest_in_dir(REVOLUT_BACKUP_DIR, "*.csv"),
        "wise":     _latest_in_dir(WISE_BACKUP_DIR, "*.zip"),
    }


def get_remote_backups() -> dict | None:
    """Return latest remote backup info from Cloudflare R2 via rclone."""
    try:
        proc = subprocess.run(
            ["rclone", "lsjson", "travelnet:travelnet"],
            capture_output=True,
            text=True,
            timeout=15,
        )
        if proc.returncode != 0:
            return {"error": proc.stderr.strip()}

        files = json.loads(proc.stdout)
        backups = sorted(
            [f for f in files if f["Name"].endswith(".db.age")],
            key=lambda f: f["ModTime"],
            reverse=True,
        )
        if not backups:
            return None

        latest = backups[0]
        mod_dt = datetime.fromisoformat(latest["ModTime"].replace("Z", "+00:00"))
        return {
            "filename":    latest["Name"],
            "size_mb":     round(latest["Size"] / (1024 * 1024), 2),
            "modified_ts": int(mod_dt.timestamp()),
            "modified":    mod_dt.strftime("%Y-%m-%d %H:%M UTC"),
            "stale":       (datetime.now(tz=timezone.utc).timestamp() - mod_dt.timestamp()) > (STALE_DAYS * 86400),
            "count":       len(backups),
        }
    except subprocess.TimeoutExpired:
        return {"error": "rclone timed out"}
    except Exception as e:
        return {"error": str(e)}
