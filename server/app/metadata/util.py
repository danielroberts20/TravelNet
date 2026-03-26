from collections import deque
from datetime import datetime, timezone
import json
import subprocess
import time
import os
from config.general import DATABASE_BACKUP_DIR, DB_FILE, FX_BACKUP_DIR, HEALTH_BACKUP_DIR, LOCATION_OVERLAND_BACKUP_DIR, LOCATION_SHORTCUTS_BACKUP_DIR, REVOLUT_BACKUP_DIR, STALE_DAYS, UPLOADS_BACKUP_DIR, WISE_BACKUP_DIR, WORKOUT_BACKUP_DIR
from config.runtime import app_start_time
from database.util import get_conn

def read_last_lines_efficient(filename: str, n: int = 200) -> str:
    """Return the last n lines of a file as a single string, using a bounded deque."""
    try:
        with open(filename, "r") as f:
            # deque with maxlen automatically keeps only last n lines
            last_lines = deque(f, maxlen=n)
            return "".join(last_lines)
    except Exception as e:
        return f"Error reading log file: {e}"
    
def _format_uptime(seconds: float) -> str:
    """Format a duration in seconds as 'Xd Yh Zm'."""
    days = int(seconds // 86400)
    hours = int((seconds % 86400) // 3600)
    minutes = int((seconds % 3600) // 60)
    return f"{days}d {hours}h {minutes}m"

def get_uptime() -> dict:
    """Return both Pi uptime (from /proc/uptime) and container/app uptime."""
    with open("/proc/uptime") as f:
        pi_seconds = float(f.read().split()[0])

    app_seconds = (datetime.now(tz=timezone.utc) - app_start_time).total_seconds()

    return {
        "pi": _format_uptime(pi_seconds),
        "app": _format_uptime(app_seconds),
    }

def get_db_stats() -> dict:
    """Return DB file size, free disk space, and a simple query latency in ms."""
    size_mb = round(os.path.getsize(DB_FILE) / (1024 * 1024), 2)

    stat = os.statvfs(DB_FILE)
    free_mb = round((stat.f_bavail * stat.f_frsize) / (1024 * 1024), 2)

    start = time.perf_counter()
    with get_conn(read_only=True) as conn:
        conn.execute("SELECT 1")
    latency_ms = round((time.perf_counter() - start) * 1000, 2)

    return {
        "size_mb": size_mb,
        "free_space_mb": free_mb,
        "query_latency_ms": latency_ms,
    }

def get_last_uploads() -> dict:
    """Return the most recent timestamp for each data source."""
    with get_conn(read_only=True) as conn:
        def latest(query: str) -> str | None:
            row = conn.execute(query).fetchone()
            return row[0] if row and row[0] else None

        return {
            "location_shortcuts": latest(
                "SELECT MAX(timestamp) FROM location_history"
            ),
            "location_overland": latest(
                "SELECT MAX(timestamp) FROM location_overland"
            ),
            "health": latest(
                "SELECT MAX(timestamp) FROM health_data"
            ),
            "transactions": latest(
                "SELECT MAX(timestamp) FROM transactions"
            ),
            "fx_rates": latest(
                "SELECT MAX(date) FROM fx_rates"
            ),
            "workouts": latest(
                "SELECT MAX(end_ts) FROM workouts"
            )
        }

def get_fx_latest_date() -> str | None:
    """Return the most recent date for which FX rates are stored."""
    with get_conn(read_only=True) as conn:
        row = conn.execute("SELECT MAX(date) FROM fx_rates").fetchone()
        return row[0] if row and row[0] else None

def get_pending_digest_count() -> int:
    """Return number of WARNING/ERROR records awaiting the next digest email."""
    with get_conn(read_only=True) as conn:
        row = conn.execute("SELECT COUNT(*) FROM log_digest").fetchone()
        return row[0] if row else 0

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
    index: Which file to return. Default is most recent file (``0``). For ``n``-th most recent file, use index ``n-1``"""
    try:
        files = sorted(directory.glob(pattern), key=lambda f: f.stat().st_mtime, reverse=True)
        if not files:
            return None
        try:
            info = _backup_info(files[index])
        except IndexError:
            info = _backup_info(files[-1]) # If desired file is out of range, use oldest file instead
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
            "overland": _latest_in_dir(LOCATION_OVERLAND_BACKUP_DIR, "*.jsonl", index=1)
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

        latest  = backups[0]
        mod_dt  = datetime.fromisoformat(latest["ModTime"].replace("Z", "+00:00"))
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