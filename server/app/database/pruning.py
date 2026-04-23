"""
database/pruning.py

Core pruning logic for removing pre-departure data.
Used by both the CLI script and the Dashboard Danger Zone.

Schema assumptions (as of June 2026 DB recreation):
  - workout_route    has ON DELETE CASCADE from workouts
  - place_visits     has ON DELETE CASCADE from known_places
  - ml_location_cluster_members has a created_at column

TABLES INTENTIONALLY NOT IN PRUNE LIST (reference / structural):
    places          — geographic reference grid; post-prune GPS points FK here immediately
    fx_rates        — keep rates for backfill_gbp coverage of surviving transactions
    api_usage       — monthly counter; pruning mid-month breaks quota tracking
    cost_of_living  — pure reference data, no timestamp column
    flights         — manually logged; keep for continuity
    sqlite_sequence — internal SQLite table
"""

from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Optional


# ---------------------------------------------------------------------------
# Table configuration
# ---------------------------------------------------------------------------

TABLE_CONFIG: dict[str, Optional[str]] = {
    # ── Misc time-series ────────────────────────────────────────────────────
    "log_digest":                  "ts",
    "trigger_log":                 "fired_at",
    "gap_annotations":             "start_ts",
    "cron_results":                "ran_at",

    # ── Finance ─────────────────────────────────────────────────────────────
    "transactions":                "timestamp",

    # ── Health ──────────────────────────────────────────────────────────────
    "health_quantity":             "timestamp",
    "health_heart_rate":           "timestamp",
    "health_sleep":                "start_ts",
    "workouts":                    "start_ts",   # workout_route auto-cascades
    "state_of_mind":               "start_ts",   # mood_labels/associations auto-cascade

    # ── Location ────────────────────────────────────────────────────────────
    "location_overland":           "timestamp",  # location_noise auto-cascades
    "location_shortcuts":          "timestamp",  # cellular_state auto-cascades

    # ── Weather ─────────────────────────────────────────────────────────────
    "weather_hourly":              "timestamp",
    "weather_daily":               "date",

    # ── Known places ────────────────────────────────────────────────────────
    "known_places":                "first_seen", # place_visits auto-cascades

    # ── ML ──────────────────────────────────────────────────────────────────
    "ml_location_cluster_members": "created_at",
    "ml_location_clusters":        "created_at",
    "ml_segments":                 "start_ts",
    "ml_anomalies":                "detected_at",

    # ── Pi / system ─────────────────────────────────────────────────────────
    "watchdog_heartbeat":          "received_at",
    "power_daily":                 "date",
    "photo_metadata":              "taken_at",

    # ── Summary / derived ───────────────────────────────────────────────────
    "daily_summary":               "date",
    "transition_timezone":         "transitioned_at",
    "country_transitions":         "entered_at",

    # ── Cascade-only (never directly deleted) ───────────────────────────────
    "mood_labels":                 None,  # → state_of_mind ON DELETE CASCADE
    "mood_associations":           None,  # → state_of_mind ON DELETE CASCADE
    "location_noise":              None,  # → location_overland ON DELETE CASCADE
    "cellular_state":              None,  # → location_shortcuts ON DELETE CASCADE
    "workout_route":               None,  # → workouts ON DELETE CASCADE
    "place_visits":                None,  # → known_places ON DELETE CASCADE
}


CASCADE_ONLY: set[str] = {
    "mood_labels",
    "mood_associations",
    "location_noise",
    "cellular_state",
    "workout_route",
    "place_visits",
}


DELETION_ORDER: list[str] = [
    # Misc
    "log_digest",
    "trigger_log",
    "gap_annotations",
    "cron_results",

    # Finance
    "transactions",

    # Health
    "health_quantity",
    "health_heart_rate",
    "health_sleep",
    "workouts",          # workout_route auto-cascades
    "state_of_mind",     # mood_labels/associations auto-cascade

    # Location
    "location_overland", # location_noise auto-cascades
    "location_shortcuts",# cellular_state auto-cascades

    # Weather
    "weather_hourly",
    "weather_daily",

    # Known places
    "known_places",      # place_visits auto-cascades

    # ML (members before clusters — no CASCADE on cluster_id FK)
    "ml_location_cluster_members",
    "ml_location_clusters",
    "ml_segments",
    "ml_anomalies",

    # Pi / system
    "watchdog_heartbeat",
    "power_daily",
    "photo_metadata",

    # Summary / derived
    "daily_summary",
    "transition_timezone",
    "country_transitions",
]

DEFAULT_TABLES: list[str] = DELETION_ORDER + list(CASCADE_ONLY)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_tables(tables: list[str]) -> None:
    """
    Raise ValueError if:
    - an unknown table name is given
    - a cascade-only table is selected without its parent
    """
    unknown = [t for t in tables if t not in TABLE_CONFIG]
    if unknown:
        raise ValueError(f"Unknown table(s): {', '.join(unknown)}")

    for cascade_table, parent in [
        ("mood_labels",       "state_of_mind"),
        ("mood_associations", "state_of_mind"),
    ]:
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

    # Direct tables — all have a timestamp column
    for table in _direct_tables_ordered(tables):
        ts_col = TABLE_CONFIG[table]
        cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {ts_col} < ?", (cutoff_str,))
        counts[table] = cur.fetchone()[0]

    # Cascade-only estimates via parent JOIN
    cascade_parent_queries = [
        ("mood_labels",       "state_of_mind",     "ml.som_id = s.id",          "mood_labels ml",       "state_of_mind s",     "s.start_ts"),
        ("mood_associations", "state_of_mind",     "ma.som_id = s.id",          "mood_associations ma", "state_of_mind s",     "s.start_ts"),
        ("location_noise",    "location_overland", "ln.overland_id = o.id",     "location_noise ln",    "location_overland o", "o.timestamp"),
        ("cellular_state",    "location_shortcuts","cs.shortcut_id = s.id",     "cellular_state cs",    "location_shortcuts s","s.timestamp"),
        ("workout_route",     "workouts",          "wr.workout_id = w.id",      "workout_route wr",     "workouts w",          "w.start_ts"),
        ("place_visits",      "known_places",      "pv.known_place_id = kp.id", "place_visits pv",      "known_places kp",     "kp.first_seen"),
    ]

    for child, parent, join_cond, child_alias, parent_alias, parent_ts in cascade_parent_queries:
        if child in tables and parent in tables:
            cur.execute(f"""
                SELECT COUNT(*) FROM {child_alias}
                JOIN {parent_alias} ON {join_cond}
                WHERE {parent_ts} < ?
            """, (cutoff_str,))
            counts[child] = cur.fetchone()[0]

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
            deleted[table] = -1

    return deleted