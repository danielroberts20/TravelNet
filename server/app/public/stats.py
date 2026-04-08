"""
public/stats.py
~~~~~~~~~~~~~~~
Query logic for the public stats endpoint.
Reads from the SQLite DB (counts only, no raw data) and travel.yml
(itinerary / current leg inference).

Nothing in this module returns raw location data, personal identifiers,
or anything beyond city-level detail.
"""

import logging
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional
import sqlite3
import yaml

from database.connection import get_conn

logger = logging.getLogger(__name__)

# Path to travel.yml — two levels up from server/app/ to repo root
TRAVEL_YML = Path(__file__).parent.parent.parent.parent.parent / "travel.yml"


# ---------------------------------------------------------------------------
# travel.yml parsing
# ---------------------------------------------------------------------------

def _load_travel_yml() -> dict:
    if not TRAVEL_YML.exists():
        logger.warning(f"travel.yml not found at {TRAVEL_YML}")
        return {}
    with open(TRAVEL_YML, "r") as f:
        return yaml.safe_load(f) or {}


def _parse_date(value) -> Optional[date]:
    """Safely parse a YAML date value (may already be a date object or a string)."""
    if value is None:
        return None
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except (ValueError, TypeError):
        return None


def _leg_arrival(leg: dict) -> Optional[date]:
    """Return best-known arrival date: actual → planned."""
    return (
        _parse_date(leg.get("actual", {}).get("arrival"))
        or _parse_date(leg.get("planned", {}).get("arrival"))
    )


def _leg_departure(leg: dict) -> Optional[date]:
    """Return best-known departure date: actual → planned. None = still there."""
    return (
        _parse_date(leg.get("actual", {}).get("departure"))
        or _parse_date(leg.get("planned", {}).get("departure"))
    )


def get_trip_status(data: dict) -> str:
    """
    Infer trip status from leg dates.
    Priority: meta.status_override → date inference.
    Returns: pre_departure | travelling | finished
    """
    override = (data.get("meta") or {}).get("status_override")
    if override in ("pre_departure", "travelling", "finished"):
        return override

    today = date.today()
    legs = data.get("legs") or []

    if not legs:
        return "pre_departure"

    # Finished = every leg has an actual departure date
    if all(
        _parse_date(leg.get("actual", {}).get("departure")) is not None
        for leg in legs
    ):
        return "finished"

    # Travelling = at least one leg's arrival has passed
    for leg in legs:
        arrival = _leg_arrival(leg)
        if arrival and today >= arrival:
            return "travelling"

    return "pre_departure"


def get_current_leg(data: dict) -> Optional[dict]:
    """
    Return the leg dict for the current leg.
    Priority: meta.current_leg_override → date inference.
    """
    meta = data.get("meta") or {}
    override_id = meta.get("current_leg_override")
    legs = data.get("legs") or []

    if override_id:
        for leg in legs:
            if leg.get("id") == override_id:
                return leg
        logger.warning(f"current_leg_override '{override_id}' not found in legs")

    today = date.today()
    for leg in legs:
        arrival = _leg_arrival(leg)
        departure = _leg_departure(leg)
        if arrival and today >= arrival:
            # Still in this leg if no departure date, or departure hasn't passed
            if departure is None or today <= departure:
                return leg

    return None


def get_days_travelling(data: dict) -> int:
    """
    Days since the first actual arrival date.
    Returns 0 if not yet departed.
    """
    legs = data.get("legs") or []
    for leg in legs:
        first_arrival = _parse_date(leg.get("actual", {}).get("arrival"))
        if first_arrival:
            return (date.today() - first_arrival).days
    return 0


def get_countries_visited(data: dict) -> dict:
    """
    Returns counts of countries visited, split by stopover status.
    A country counts as visited once its leg arrival date has passed.
    SE Asia (country=null) is counted as 1 if the leg has started.
    """
    today = date.today()
    legs = data.get("legs") or []

    full = 0
    stopover = 0

    for leg in legs:
        arrival = _leg_arrival(leg)
        if not arrival or today < arrival:
            continue
        if leg.get("stopover"):
            stopover += 1
        else:
            full += 1

    return {"full": full, "stopover": stopover, "total": full + stopover}


# ---------------------------------------------------------------------------
# DB queries — counts only, no raw data
# ---------------------------------------------------------------------------

def _fetchone(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> int:
    row = conn.execute(sql, params).fetchone()
    return row[0] if row and row[0] is not None else 0


def get_db_stats() -> dict:
    """
    Query the DB for public-safe aggregate counts.
    Returns a dict of counts and last_synced timestamp.
    """
    try:
        with get_conn() as conn:
            gps_points = _fetchone(
                conn,
                "SELECT COUNT(*) FROM location_unified"
            )
            health_records = _fetchone(
                conn,
                "SELECT COUNT(*) FROM health_quantity"
            )
            transactions = _fetchone(
                conn,
                "SELECT COUNT(*) FROM transactions"
            )
            # Last synced = most recent location point timestamp
            row = conn.execute(
                "SELECT MAX(timestamp) FROM location_unified"
            ).fetchone()
            last_synced_raw = row[0] if row else None

        last_synced = None
        if last_synced_raw:
            try:
                dt = datetime.fromisoformat(last_synced_raw.replace("Z", "+00:00"))
                last_synced = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            except (ValueError, AttributeError):
                pass

        return {
            "gps_points": gps_points,
            "health_records": health_records,
            "transactions": transactions,
            "last_synced": last_synced,
        }

    except Exception as e:
        logger.error(f"Failed to query DB for public stats: {e}")
        return {
            "gps_points": 0,
            "health_records": 0,
            "transactions": 0,
            "last_synced": None,
        }


# ---------------------------------------------------------------------------
# Assembled public payload
# ---------------------------------------------------------------------------

def build_public_stats() -> dict:
    """
    Assemble the full public stats payload.
    Safe to expose: counts, city-level location, leg metadata only.
    """
    data = _load_travel_yml()
    db = get_db_stats()
    countries = get_countries_visited(data)
    current_leg = get_current_leg(data)
    status = get_trip_status(data)

    payload = {
        "status": status,
        "days_travelling": get_days_travelling(data),
        "countries_visited": countries["full"],
        "stopover_countries": countries["stopover"],
        "total_countries": countries["total"],
        "current_leg": None,
        "gps_points": db["gps_points"],
        "health_records": db["health_records"],
        "transactions": db["transactions"],
        "last_synced": db["last_synced"],
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    if current_leg:
        payload["current_leg"] = {
            "id": current_leg.get("id"),
            "name": current_leg.get("name"),
            "emoji": current_leg.get("emoji"),
            "stopover": current_leg.get("stopover", False),
        }

    return payload