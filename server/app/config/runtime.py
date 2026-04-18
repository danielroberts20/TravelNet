"""
config/runtime.py
~~~~~~~~~~~~~~~~~
Records the moment the application process started.
Call initialise() once from main.py lifespan. All other imports
use get_app_uptime() which reads from the shared file.
"""
from datetime import datetime, timezone
import os

_runtime_file = "/data/app_start_time"

def initialise() -> None:
    """Write the current time to the runtime file. Call once at startup."""
    now = datetime.now(tz=timezone.utc)
    try:
        with open(_runtime_file, "w") as f:
            f.write(now.isoformat())
    except Exception:
        pass  # don't block startup if write fails

def get_app_uptime() -> float | None:
    """Returns the app uptime in seconds, or None if start time is unavailable."""
    try:
        with open(_runtime_file, "r") as f:
            start_time = datetime.fromisoformat(f.read().strip())
        return (datetime.now(tz=timezone.utc) - start_time).total_seconds()
    except Exception:
        return None