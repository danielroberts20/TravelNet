"""
metadata/system.py
~~~~~~~~~~~~~~~~~~
System health helpers for the metadata dashboard endpoint.

Provides:
  - read_last_lines_efficient() — tail a log file without reading the whole thing
  - get_uptime()                — Pi and container uptime
  - get_db_stats()              — DB file size, free disk, and query latency
  - get_pending_digest_count()  — WARNING/ERROR records awaiting the daily digest
"""

from collections import deque
from datetime import datetime, timezone
import os
import time

from config.general import DB_FILE
from config.runtime import app_start_time
from database.connection import get_conn


def read_last_lines_efficient(filename: str, n: int = 200) -> str:
    """Return the last n lines of a file as a single string, using a bounded deque."""
    try:
        with open(filename, "r") as f:
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


def get_pending_digest_count() -> int:
    """Return number of WARNING/ERROR records awaiting the next digest email."""
    with get_conn(read_only=True) as conn:
        row = conn.execute("SELECT COUNT(*) FROM log_digest").fetchone()
        return row[0] if row else 0
