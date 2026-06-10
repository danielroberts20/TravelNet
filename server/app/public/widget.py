"""
public/widget.py
~~~~~~~~~~~~~~~~
Query logic for the public widget endpoint.
Returns system health and current trip context for the Scriptable iOS widget.
No raw location data or personal identifiers are exposed.
"""

import logging
import os
from datetime import datetime, timedelta, timezone

from config.general import DATABASE_BACKUP_DIR, DB_FILE
from database.connection import get_conn
from public.stats import _load_travel_yml, get_current_leg, get_days_travelling
from util import time_ago

logger = logging.getLogger(__name__)

_WATCHDOG_STALE_MINUTES = 15


def _get_system_ok(conn) -> bool | None:
    """
    True if a watchdog heartbeat arrived within the last _WATCHDOG_STALE_MINUTES
    minutes AND the most recent row has api_ok = 1. Mirrors the staleness check
    in check_watchdog.py (threshold_minutes=10 there; 15 here for display tolerance).
    """
    try:
        row = conn.execute("""
            SELECT received_at, api_ok
            FROM watchdog_heartbeat
            ORDER BY received_at DESC
            LIMIT 1
        """).fetchone()
        if row is None:
            return None
        received_at = datetime.fromisoformat(row["received_at"].replace("Z", "+00:00"))
        age = datetime.now(timezone.utc) - received_at
        if age > timedelta(minutes=_WATCHDOG_STALE_MINUTES):
            return False
        return bool(row["api_ok"])
    except Exception as e:
        logger.warning("Failed to check watchdog health: %s", e)
        return None


def _latest_ts(conn, query: str) -> str | None:
    try:
        row = conn.execute(query).fetchone()
        return row[0] if row and row[0] else None
    except Exception as e:
        logger.warning("Failed to fetch timestamp (%s): %s", query, e)
        return None


def _last_backup_ts() -> str | None:
    """ISO timestamp of the most recently modified .db backup file, or None."""
    try:
        files = sorted(
            DATABASE_BACKUP_DIR.glob("*.db.zst"),
            key=lambda f: f.stat().st_mtime,
            reverse=True,
        )
        if not files:
            return None
        mtime = files[0].stat().st_mtime
        return datetime.fromtimestamp(mtime, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception as e:
        logger.warning("Failed to read backup directory: %s", e)
        return None


def _db_size_mb() -> float | None:
    try:
        return round(os.path.getsize(DB_FILE) / (1024 * 1024), 1)
    except Exception as e:
        logger.warning("Failed to get DB size: %s", e)
        return None


def build_widget_data() -> dict:
    """Assemble the full widget payload. All fields are nullable."""
    data = _load_travel_yml()
    current_leg = get_current_leg(data)

    current_leg_name = current_leg.get("name") if current_leg else None
    country = current_leg.get("country") if current_leg else None

    try:
        with get_conn(read_only=True) as conn:
            system_ok = _get_system_ok(conn)
            last_location_ts = _latest_ts(conn, "SELECT MAX(timestamp) FROM location_overland")
            last_health_ts = _latest_ts(conn, "SELECT MAX(timestamp) FROM health_quantity")
            last_watchdog_ts = _latest_ts(
                conn,
                "SELECT MAX(received_at) FROM watchdog_heartbeat",
            )
    except Exception as e:
        logger.error("Failed to query DB for widget data: %s", e)
        system_ok = None
        last_location_ts = last_health_ts = last_watchdog_ts = None

    return {
        "system_ok": system_ok,
        "current_leg": current_leg_name,
        "country": country,
        "last_location": time_ago(last_location_ts),
        "last_health": time_ago(last_health_ts),
        "last_watchdog": time_ago(last_watchdog_ts),
        "last_backup": time_ago(_last_backup_ts()),
        "days_travelling": get_days_travelling(data),
        "db_size_mb": _db_size_mb(),
    }
