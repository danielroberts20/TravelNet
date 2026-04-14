"""
database/pruning.py

Core pruning logic for removing pre-departure data.
Used by both the CLI script and the dashboard Danger Zone.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Optional


# ---------------------------------------------------------------------------
# Table configuration
# ---------------------------------------------------------------------------

# Maps table name -> timestamp column.
# Deletion order matters for tables with FK relationships that lack ON DELETE
# CASCADE — children must be deleted before parents.
#
# Cascade notes from schema:
#   - cellular_state              → CASCADE from location_shortcuts  (not in prune list)
#   - ml_location_cluster_members → CASCADE from location_overland   (handled automatically)
#   - mood_labels                 → CASCADE from state_of_mind       (handled automatically)
#   - mood_associations           → CASCADE from state_of_mind       (handled automatically)
#   - workout_route               → FK to workouts, NO CASCADE       → delete explicitly first
#   - place_visits                → FK to known_places, NO CASCADE   → delete explicitly first

TABLE_CONFIG: dict[str, str] = {
    "log_digest":         "ts",
    "trigger_log":        "fired_at",
    "gap_annotations":    "start_ts",
    "transactions":       "timestamp",
    "health_quantity":    "timestamp",
    "health_heart_rate":  "timestamp",
    "health_sleep":       "start_ts",
    "workout_route":      "timestamp",   # must precede workouts (no CASCADE)
    "workouts":           "start_ts",
    "state_of_mind":      "start_ts",    # mood_labels/associations cascade automatically
    "mood_labels":        None,          # cascade only
    "mood_associations":  None,          # cascade only
    "location_overland":  "timestamp",
    "location_shortcuts": "timestamp",
    "weather_hourly":     "timestamp",
    "weather_daily":      "date",
    "place_visits":       "arrived_at",  # must precede known_places (no CASCADE)
    "known_places":       "first_seen",
}

# Tables deleted via parent CASCADE — never directly deleted.
CASCADE_ONLY = {"mood_labels", "mood_associations"}

# Explicit FK-safe deletion order for all non-cascade tables.
DELETION_ORDER: list[str] = [
    "log_digest",
    "trigger_log",
    "gap_annotations",
    "transactions",
    "health_quantity",
    "health_heart_rate",
    "health_sleep",
    "workout_route",        # before workouts
    "workouts",
    "state_of_mind",        # mood_labels/associations auto-cascade
    "location_overland",
    "location_shortcuts",
    "weather_hourly",
    "weather_daily",
    "place_visits",         # before known_places
    "known_places",
]

DEFAULT_TABLES: list[str] = DELETION_ORDER + list(CASCADE_ONLY)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_tables(tables: list[str]) -> None:
    """
    Raise ValueError if:
    - an unknown table name is given
    - mood_labels or mood_associations are selected without state_of_mind
    """
    unknown = [t for t in tables if t not in TABLE_CONFIG]
    if unknown:
        raise ValueError(f"Unknown table(s): {', '.join(unknown)}")

    for cascade_table, parent in [("mood_labels", "state_of_mind"), ("mood_associations", "state_of_mind")]:
        if cascade_table in tables and parent not in tables:
            raise ValueError(
                f"'{cascade_table}' is deleted via CASCADE from '{parent}'. "
                f"Include '{parent}' or remove '{cascade_table}'."
            )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _direct_tables_ordered(tables: list[str]) -> list[str]:
    """Return non-cascade tables in FK-safe deletion order."""
    selected = set(tables) - CASCADE_ONLY
    return [t for t in DELETION_ORDER if t in selected]


def _parse_cutoff(cutoff: str | datetime) -> str:
    """Normalise cutoff to an ISO8601 string for SQLite comparison."""
    if isinstance(cutoff, datetime):
        return cutoff.isoformat()
    datetime.fromisoformat(cutoff)  # validate
    return cutoff


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_prune_counts(
    conn: sqlite3.Connection,
    cutoff: str | datetime,
    tables: Optional[list[str]] = None,
) -> dict[str, int]:
    """
    Return the number of rows that would be deleted from each table.
    Cascade-only tables are estimated via a JOIN on their parent.
    Results are returned in deletion order.
    """
    if tables is None:
        tables = DEFAULT_TABLES
    validate_tables(tables)
    cutoff_str = _parse_cutoff(cutoff)

    counts: dict[str, int] = {}
    cur = conn.cursor()

    for table in _direct_tables_ordered(tables):
        ts_col = TABLE_CONFIG[table]
        cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {ts_col} < ?", (cutoff_str,))
        counts[table] = cur.fetchone()[0]

    if "mood_labels" in tables and "state_of_mind" in tables:
        cur.execute(
            """
            SELECT COUNT(*) FROM mood_labels ml
            JOIN state_of_mind s ON ml.som_id = s.id
            WHERE s.start_ts < ?
            """,
            (cutoff_str,),
        )
        counts["mood_labels"] = cur.fetchone()[0]

    if "mood_associations" in tables and "state_of_mind" in tables:
        cur.execute(
            """
            SELECT COUNT(*) FROM mood_associations ma
            JOIN state_of_mind s ON ma.som_id = s.id
            WHERE s.start_ts < ?
            """,
            (cutoff_str,),
        )
        counts["mood_associations"] = cur.fetchone()[0]

    return counts


def prune_before(
    conn: sqlite3.Connection,
    cutoff: str | datetime,
    tables: Optional[list[str]] = None,
) -> dict[str, int]:
    """
    Delete all rows with a timestamp before `cutoff` from each table.
    Runs in FK-safe order. Cascade-only tables are not directly deleted —
    SQLite handles them automatically via ON DELETE CASCADE.

    Returns {table: rows_deleted}. Cascade-only tables return -1.
    """
    if tables is None:
        tables = DEFAULT_TABLES
    validate_tables(tables)
    cutoff_str = _parse_cutoff(cutoff)

    deleted: dict[str, int] = {}
    cur = conn.cursor()

    for table in _direct_tables_ordered(tables):
        ts_col = TABLE_CONFIG[table]
        cur.execute(f"DELETE FROM {table} WHERE {ts_col} < ?", (cutoff_str,))
        deleted[table] = cur.rowcount

    conn.commit()

    for table in CASCADE_ONLY:
        if table in tables:
            deleted[table] = -1  # handled by parent cascade

    return deleted