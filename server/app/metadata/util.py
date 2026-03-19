from collections import deque
from datetime import datetime, timezone
import time
import os
from config.database import DB_FILE
from config.runtime import app_start_time
from database.util import get_conn

def read_last_lines_efficient(filename: str, n: int = 200) -> str:
    try:
        with open(filename, "r") as f:
            # deque with maxlen automatically keeps only last n lines
            last_lines = deque(f, maxlen=n)
            return "".join(last_lines)
    except Exception as e:
        return f"Error reading log file: {e}"
    
def _format_uptime(seconds: float) -> str:
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
                "SELECT MAX(created_at) FROM fx_rates"
            ),
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