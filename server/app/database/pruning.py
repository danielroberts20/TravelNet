"""
database/pruning.py

Schema-driven pruning engine for removing pre-departure data.
Used by both the CLI script and the Dashboard Danger Zone.

Table inventory, timestamp columns, and FK relationships are discovered at
import time from SQLite's pragma_table_info / pragma_foreign_key_list.
If DB_FILE does not exist (e.g. during unit tests), hardcoded fallback
constants matching the known schema are used so callers stay operational.
"""

from __future__ import annotations

import logging
import sqlite3
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Optional

from config.general import DB_FILE

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration  (the only manually maintained section)
# ---------------------------------------------------------------------------

# Tables permanently excluded from pruning — reference/structural data or
# tables with no meaningful timestamp semantics.
EXCLUDE_TABLES: set[str] = {
    "places",
    "fx_rates",
    "api_usage",
    "cost_of_living",
    "flights",
    "sqlite_sequence",
    "sqlite_stat1",
    "sqlite_stat2",
    "sqlite_stat3",
    "sqlite_stat4",
}

# Preferred column names tried in order; first match wins.
TS_COLUMN_PREFERENCE: list[str] = [
    "timestamp", "ts", "fired_at", "start_ts", "date",
    "ran_at", "received_at", "detected_at", "taken_at",
    "entered_at", "transitioned_at", "first_seen",
]

# FK edges to ignore when building the dependency graph for topological sort.
# Use this for circular FKs where one direction is a soft pointer (no cascade)
# and the other is the structurally meaningful cascade direction.
# Format: {(child_table, parent_table)} — the edge to DROP from the graph.
# The ignored edge should always be the non-cascade direction.
FK_IGNORE: set[tuple[str, str]] = {
    ("known_places", "place_visits"),  # current_visit_id is a soft pointer;
                                       # place_visits → known_places CASCADE
                                       # already establishes correct order
}

# Explicit overrides for tables where the preference order would pick the
# wrong column, or where a non-preference column must be used.
TS_COLUMN_OVERRIDE: dict[str, str] = {
    "gap_annotations":             "start_ts",
    "health_sleep":                "start_ts",
    "workouts":                    "start_ts",
    "state_of_mind":               "start_ts",
    "ml_segments":                 "start_ts",
    "place_visits":                "arrived_at",
    "country_transitions":         "entered_at",
    "transition_timezone":         "transitioned_at",
    # ml_location_cluster_members has a cascade FK to location_overland but is
    # deleted directly via created_at — the override prevents cascade-only classification.
    "ml_location_cluster_members": "created_at",
    "ml_location_clusters": "created_at",
    "ml_destination_profiles": "created_at",
    "ml_causal_graph": "created_at",
}


# ---------------------------------------------------------------------------
# Hardcoded fallback — used when DB_FILE is absent (e.g. test environments)
# ---------------------------------------------------------------------------
# These mirror the known production schema so that the existing test suite
# continues to work against the public API without a live database.

_FALLBACK_TABLE_CONFIG: dict[str, Optional[str]] = {
    "log_digest":                  "ts",
    "trigger_log":                 "fired_at",
    "gap_annotations":             "start_ts",
    "cron_results":                "ran_at",
    "transactions":                "timestamp",
    "health_quantity":             "timestamp",
    "health_heart_rate":           "timestamp",
    "health_sleep":                "start_ts",
    "workouts":                    "start_ts",
    "state_of_mind":               "start_ts",
    "location_overland":           "timestamp",
    "location_shortcuts":          "timestamp",
    "weather_hourly":              "timestamp",
    "weather_daily":               "date",
    "weather_fetch_log":           "date",
    "known_places":                "first_seen",
    "ml_location_cluster_members": "created_at",
    "ml_location_clusters":        "created_at",
    "ml_segments":                 "start_ts",
    "ml_anomalies":                "detected_at",
    "watchdog_heartbeat":          "received_at",
    "power_daily":                 "date",
    "photo_metadata":              "taken_at",
    "daily_summary":               "date",
    "transition_timezone":         "transitioned_at",
    "country_transitions":         "entered_at",
    # cascade-only
    "mood_labels":                 None,
    "mood_associations":           None,
    "location_noise":              None,
    "cellular_state":              None,
    "workout_route":               None,
    "place_visits":                None,
}

_FALLBACK_CASCADE_ONLY: set[str] = {
    "mood_labels",
    "mood_associations",
    "location_noise",
    "cellular_state",
    "workout_route",
    "place_visits",
}

_FALLBACK_DELETION_ORDER: list[str] = [
    "log_digest",
    "trigger_log",
    "gap_annotations",
    "cron_results",
    "transactions",
    "health_quantity",
    "health_heart_rate",
    "health_sleep",
    "workouts",
    "state_of_mind",
    "location_overland",
    "location_shortcuts",
    "weather_hourly",
    "weather_daily",
    "weather_fetch_log",
    "known_places",
    "ml_location_cluster_members",
    "ml_location_clusters",
    "ml_segments",
    "ml_anomalies",
    "watchdog_heartbeat",
    "power_daily",
    "photo_metadata",
    "daily_summary",
    "transition_timezone",
    "country_transitions",
]

# child: (parent_table, child_fk_col, parent_pk_col)
_FALLBACK_CASCADE_FK_INFO: dict[str, tuple[str, str, str]] = {
    "mood_labels":       ("state_of_mind",     "som_id",         "id"),
    "mood_associations": ("state_of_mind",     "som_id",         "id"),
    "location_noise":    ("location_overland", "overland_id",    "id"),
    "cellular_state":    ("location_shortcuts","shortcut_id",    "id"),
    "workout_route":     ("workouts",          "workout_id",     "id"),
    "place_visits":      ("known_places",      "known_place_id", "id"),
}


# ---------------------------------------------------------------------------
# Schema discovery
# ---------------------------------------------------------------------------

_EMPTY_SCHEMA: tuple[
    dict[str, Optional[str]],
    set[str],
    list[str],
    list[str],
    dict[str, tuple[str, str, str]],
] = ({}, set(), [], [], {})


def _build_schema(
    conn: Optional[sqlite3.Connection] = None,
) -> tuple[
    dict[str, Optional[str]],   # TABLE_CONFIG
    set[str],                   # CASCADE_ONLY
    list[str],                  # DELETION_ORDER
    list[str],                  # DEFAULT_TABLES
    dict[str, tuple[str, str, str]],  # cascade_fk_info: child → (parent, child_col, parent_col)
]:
    """
    Discover tables, timestamp columns, and FK relationships from the DB.

    Pass a sqlite3.Connection to use an existing connection (e.g. an in-memory
    DB in tests).  When conn is None the function opens DB_FILE read-only.

    Returns empty structures (not the hardcoded fallback) if:
    - conn is None and DB_FILE does not exist
    - discovery raises an unexpected exception

    The module-level constants TABLE_CONFIG / CASCADE_ONLY / DELETION_ORDER /
    DEFAULT_TABLES are initialised with the hardcoded fallback when this
    function returns empty, so callers that import these constants continue to
    work even without a database file.

    Returns a 5-tuple: (TABLE_CONFIG, CASCADE_ONLY, DELETION_ORDER,
                        DEFAULT_TABLES, cascade_fk_info).
    """
    close_after = False
    if conn is None:
        db_path = Path(DB_FILE)
        if not db_path.exists():
            logger.warning(
                "pruning: DB_FILE not found at %s — schema discovery skipped",
                db_path,
            )
            return _EMPTY_SCHEMA
        try:
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
            conn.row_factory = sqlite3.Row
            close_after = True
        except Exception as exc:
            logger.warning("pruning: cannot open DB (%s) — schema discovery skipped", exc)
            return _EMPTY_SCHEMA
    else:
        # Ensure Row factory so columns are accessible by name
        conn.row_factory = sqlite3.Row

    try:
        return _discover(conn)
    except RuntimeError:
        raise
    except Exception as exc:
        logger.warning("pruning: schema discovery failed (%s)", exc)
        return _EMPTY_SCHEMA
    finally:
        if close_after:
            conn.close()


def _discover(
    conn: sqlite3.Connection,
) -> tuple[
    dict[str, Optional[str]],
    set[str],
    list[str],
    list[str],
    dict[str, tuple[str, str, str]],
]:
    """Run the actual PRAGMA-based schema discovery against an open connection."""
    # 1. All real tables (not views, not internal sqlite_ tables)
    all_tables: list[str] = [
        row["name"]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        if row["name"] not in EXCLUDE_TABLES and not row["name"].startswith("sqlite_")
    ]

    # 2. Per-table: column names and outgoing FK list
    table_cols: dict[str, list[str]] = {}
    table_fks: dict[str, list[dict]] = {}

    for tname in all_tables:
        table_cols[tname] = [
            row["name"]
            for row in conn.execute(f"PRAGMA table_info([{tname}])").fetchall()
        ]
        table_fks[tname] = [
            {
                "parent":    row["table"],
                "child_col": row["from"],
                "parent_col": row["to"],
                "on_delete": (row["on_delete"] or "").upper(),
            }
            for row in conn.execute(f"PRAGMA foreign_key_list([{tname}])").fetchall()
        ]

    # 3. Determine ts_col for each table and detect cascade-only tables
    table_config: dict[str, Optional[str]] = {}
    cascade_only: set[str] = set()
    cascade_fk_info: dict[str, tuple[str, str, str]] = {}

    # First pass: determine ts_col
    ts_col_map: dict[str, Optional[str]] = {}
    ts_col_is_fallback: dict[str, bool] = {}

    for tname, cols in table_cols.items():
        col_set = set(cols)

        # Override takes priority
        if tname in TS_COLUMN_OVERRIDE:
            override_col = TS_COLUMN_OVERRIDE[tname]
            if override_col in col_set:
                ts_col_map[tname] = override_col
                ts_col_is_fallback[tname] = False
                continue
            # Override column not present in table — fall through

        # Scan preference list
        found = None
        for pref in TS_COLUMN_PREFERENCE:
            if pref in col_set:
                found = pref
                break

        if found is not None:
            ts_col_map[tname] = found
            ts_col_is_fallback[tname] = False
        elif "created_at" in col_set:
            ts_col_map[tname] = "created_at"
            ts_col_is_fallback[tname] = True
            logger.warning(
                "pruning: %s has no preferred timestamp column, using 'created_at'",
                tname,
            )
        else:
            ts_col_map[tname] = None
            ts_col_is_fallback[tname] = True

    # Second pass: classify cascade-only tables.
    # A table is cascade-only if:
    #   (a) it declares an ON DELETE CASCADE FK to some parent, AND
    #   (b) its ts_col was not found via override or preference (only created_at
    #       fallback or nothing) — meaning it has no independent time axis useful
    #       for direct pruning.
    for tname, fks in table_fks.items():
        for fk in fks:
            if fk["on_delete"] == "CASCADE" and ts_col_is_fallback.get(tname, True):
                parent = fk["parent"]
                cascade_only.add(tname)
                table_config[tname] = None
                if tname not in cascade_fk_info:  # first CASCADE FK wins
                    cascade_fk_info[tname] = (
                        parent,
                        fk["child_col"],
                        fk["parent_col"],
                    )
                break

    # Third pass: populate table_config for non-cascade-only tables
    for tname, ts_col in ts_col_map.items():
        if tname in cascade_only:
            continue
        if ts_col is None:
            # No timestamp at all and not cascade-only — skip with a warning
            logger.warning(
                "pruning: %s has no usable timestamp column — skipping", tname
            )
            continue
        table_config[tname] = ts_col

    # 4. Topological sort (children before parents) via Kahn's algorithm
    #    Only sort tables with a direct ts_col (non-cascade-only).
    direct_tables = {t for t, ts in table_config.items() if ts is not None}

    in_degree: dict[str, int] = {t: 0 for t in direct_tables}
    edges_from: dict[str, list[str]] = {t: [] for t in direct_tables}

    for tname in direct_tables:
        for fk in table_fks.get(tname, []):
            parent = fk["parent"]
            if parent in direct_tables and parent != tname:
                if (tname, parent) in FK_IGNORE:
                    logger.debug(
                        "pruning: skipping FK edge (%s → %s) — registered in FK_IGNORE",
                        tname, parent,
                    )
                    continue
                edges_from[tname].append(parent)
                in_degree[parent] += 1

    queue: deque[str] = deque(
        sorted(t for t in direct_tables if in_degree[t] == 0)
    )
    deletion_order: list[str] = []

    while queue:
        node = queue.popleft()
        deletion_order.append(node)
        for neighbour in sorted(edges_from.get(node, [])):
            in_degree[neighbour] -= 1
            if in_degree[neighbour] == 0:
                queue.append(neighbour)

    if len(deletion_order) != len(direct_tables):
        remaining = sorted(direct_tables - set(deletion_order))
        raise RuntimeError(
            f"pruning: unregistered FK cycle detected involving {remaining}. "
            f"Add the appropriate edge to FK_IGNORE to resolve."
        )

    default_tables = deletion_order + list(cascade_only)
    return table_config, cascade_only, deletion_order, default_tables, cascade_fk_info


# ---------------------------------------------------------------------------
# Module-level public constants  (rebuilt at import time)
# ---------------------------------------------------------------------------
# _build_schema() returns empty structures when DB_FILE is absent.
# In that case we fall back to the hardcoded constants so that callers which
# operate without a live database (unit tests, Docker build-time imports) get
# a fully functional module rather than an unusable empty one.

_discovered = _build_schema()

if _discovered[0]:  # non-empty TABLE_CONFIG → dynamic discovery succeeded
    (
        TABLE_CONFIG,
        CASCADE_ONLY,
        DELETION_ORDER,
        DEFAULT_TABLES,
        _CASCADE_FK_INFO,
    ) = _discovered
else:
    TABLE_CONFIG    = dict(_FALLBACK_TABLE_CONFIG)
    CASCADE_ONLY    = set(_FALLBACK_CASCADE_ONLY)
    DELETION_ORDER  = list(_FALLBACK_DELETION_ORDER)
    DEFAULT_TABLES  = list(_FALLBACK_DELETION_ORDER) + list(_FALLBACK_CASCADE_ONLY)
    _CASCADE_FK_INFO = dict(_FALLBACK_CASCADE_FK_INFO)

# Convenience map: cascade-only table → its parent (used by validate_tables)
_CASCADE_PARENTS: dict[str, str] = {
    child: info[0] for child, info in _CASCADE_FK_INFO.items()
}


# ---------------------------------------------------------------------------
# Internal helpers
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

    tables_set = set(tables)
    for child in CASCADE_ONLY:
        if child not in tables_set:
            continue
        parent = _CASCADE_PARENTS.get(child)
        if parent and parent not in tables_set:
            raise ValueError(
                f"'{child}' is deleted via CASCADE from '{parent}'. "
                f"Include '{parent}' or remove '{child}'."
            )


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

    # Direct tables
    for table in _direct_tables_ordered(tables):
        ts_col = TABLE_CONFIG[table]
        cur.execute(
            f"SELECT COUNT(*) FROM [{table}] WHERE [{ts_col}] < ?",
            (cutoff_str,),
        )
        counts[table] = cur.fetchone()[0]

    # Cascade-only: estimate via JOIN on parent's timestamp
    tables_set = set(tables)
    for child in CASCADE_ONLY:
        if child not in tables_set:
            continue
        fk_info = _CASCADE_FK_INFO.get(child)
        if not fk_info:
            continue
        parent, child_col, parent_col = fk_info
        if parent not in tables_set:
            continue
        parent_ts = TABLE_CONFIG.get(parent)
        if not parent_ts:
            continue
        cur.execute(
            f"SELECT COUNT(*) FROM [{child}]"
            f" JOIN [{parent}] ON [{child}].[{child_col}] = [{parent}].[{parent_col}]"
            f" WHERE [{parent}].[{parent_ts}] < ?",
            (cutoff_str,),
        )
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
        cur.execute(
            f"DELETE FROM [{table}] WHERE [{ts_col}] < ?",
            (cutoff_str,),
        )
        deleted[table] = cur.rowcount

    conn.commit()

    tables_set = set(tables)
    for table in CASCADE_ONLY:
        if table in tables_set:
            deleted[table] = -1

    return deleted
