"""
test_pruning_dynamic.py

Tests for the schema-driven pruning engine (_build_schema and the public API
exercised against an in-memory SQLite DB rather than /data/travel.db).

In-memory schema covers:
  - location_overland  — preference-list timestamp column (timestamp)
  - workouts           — TS_COLUMN_OVERRIDE (start_ts)
  - workout_route      — cascade-only, ON DELETE CASCADE from workouts
  - audit_log          — only created_at, no cascade parent → fallback with warning
  - places             — in EXCLUDE_TABLES; must be absent from TABLE_CONFIG
"""

import logging
import sqlite3
import unittest.mock as mock

import pytest

import database.pruning as pruning_mod
from database.pruning import (
    _build_schema,
    get_prune_counts,
    prune_before,
    validate_tables,
)


# ---------------------------------------------------------------------------
# Shared in-memory DB
# ---------------------------------------------------------------------------

_SCHEMA_SQL = """
    CREATE TABLE location_overland (
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT NOT NULL,
        latitude  REAL,
        longitude REAL
    );

    CREATE TABLE workouts (
        id       INTEGER PRIMARY KEY AUTOINCREMENT,
        start_ts TEXT NOT NULL,
        name     TEXT
    );

    CREATE TABLE workout_route (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        workout_id INTEGER NOT NULL
                           REFERENCES workouts(id) ON DELETE CASCADE,
        lat        REAL,
        lon        REAL
    );

    CREATE TABLE audit_log (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT NOT NULL,
        message    TEXT
    );

    CREATE TABLE places (
        id   INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT
    );
"""


@pytest.fixture
def mem_conn():
    """Open an in-memory DB, build the representative schema, yield the connection."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(_SCHEMA_SQL)
    return conn


@pytest.fixture
def schema(mem_conn):
    """
    Build schema against the in-memory DB and temporarily patch the module-level
    constants so every test in this class sees the dynamic values.
    """
    tc, co, do, dt, fk_info = _build_schema(mem_conn)
    cascade_parents = {child: info[0] for child, info in fk_info.items()}
    with (
        mock.patch.object(pruning_mod, "TABLE_CONFIG", tc),
        mock.patch.object(pruning_mod, "CASCADE_ONLY", co),
        mock.patch.object(pruning_mod, "DELETION_ORDER", do),
        mock.patch.object(pruning_mod, "DEFAULT_TABLES", dt),
        mock.patch.object(pruning_mod, "_CASCADE_FK_INFO", fk_info),
        mock.patch.object(pruning_mod, "_CASCADE_PARENTS", cascade_parents),
    ):
        yield tc, co, do, dt, fk_info


# ---------------------------------------------------------------------------
# 1. _build_schema produces correct TABLE_CONFIG / CASCADE_ONLY / DELETION_ORDER
# ---------------------------------------------------------------------------

class TestBuildSchema:

    def test_preference_list_table_present(self, schema):
        tc, *_ = schema
        assert "location_overland" in tc
        assert tc["location_overland"] == "timestamp"

    def test_override_table_present(self, schema):
        tc, *_ = schema
        assert "workouts" in tc
        assert tc["workouts"] == "start_ts"

    def test_cascade_only_table_has_none_ts(self, schema):
        tc, co, *_ = schema
        assert "workout_route" in co
        assert tc.get("workout_route") is None

    def test_created_at_fallback_table_is_direct(self, schema):
        tc, co, *_ = schema
        # audit_log has only created_at and no cascade FK → directly prunable
        assert "audit_log" in tc
        assert tc["audit_log"] == "created_at"
        assert "audit_log" not in co

    def test_excluded_table_absent(self, schema):
        tc, co, *_ = schema
        assert "places" not in tc
        assert "places" not in co

    def test_cascade_only_workout_route_in_cascade_set(self, schema):
        _, co, *_ = schema
        assert "workout_route" in co

    def test_created_at_fallback_logs_warning(self, mem_conn, caplog):
        with caplog.at_level(logging.WARNING, logger="database.pruning"):
            _build_schema(mem_conn)
        assert any("created_at" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# 2. DELETION_ORDER has children before parents
# ---------------------------------------------------------------------------

class TestDeletionOrder:

    def test_workout_route_not_in_deletion_order(self, schema):
        _, co, do, *_ = schema
        # cascade-only tables are excluded from DELETION_ORDER
        assert "workout_route" not in do

    def test_workouts_in_deletion_order(self, schema):
        _, _, do, *_ = schema
        assert "workouts" in do

    def test_location_overland_in_deletion_order(self, schema):
        _, _, do, *_ = schema
        assert "location_overland" in do


# ---------------------------------------------------------------------------
# 3. validate_tables
# ---------------------------------------------------------------------------

class TestValidateTables:

    def test_unknown_table_raises(self, schema):
        with pytest.raises(ValueError, match="Unknown table"):
            validate_tables(["nonexistent_table"])

    def test_cascade_only_without_parent_raises(self, schema):
        with pytest.raises(ValueError, match="workouts"):
            validate_tables(["workout_route"])

    def test_cascade_only_with_parent_passes(self, schema):
        validate_tables(["workouts", "workout_route"])

    def test_direct_table_passes(self, schema):
        validate_tables(["location_overland"])

    def test_empty_list_passes(self, schema):
        validate_tables([])


# ---------------------------------------------------------------------------
# 4. get_prune_counts returns correct counts
# ---------------------------------------------------------------------------

class TestGetPruneCounts:

    def _seed(self, conn):
        conn.execute(
            "INSERT INTO location_overland (timestamp) VALUES (?), (?), (?)",
            ("2024-01-01T00:00:00", "2024-01-02T00:00:00", "2025-01-01T00:00:00"),
        )
        old_id = conn.execute(
            "INSERT INTO workouts (start_ts, name) VALUES (?, ?)",
            ("2024-02-01T00:00:00", "run"),
        ).lastrowid
        conn.execute(
            "INSERT INTO workouts (start_ts, name) VALUES (?, ?)",
            ("2025-02-01T00:00:00", "future run"),
        )
        conn.execute(
            "INSERT INTO workout_route (workout_id, lat, lon) VALUES (?, ?, ?)",
            (old_id, 51.5, -0.1),
        )
        conn.commit()
        return old_id

    def test_direct_table_count(self, schema, mem_conn):
        self._seed(mem_conn)
        counts = get_prune_counts(
            mem_conn, "2024-12-01T00:00:00", tables=["location_overland"]
        )
        assert counts["location_overland"] == 2

    def test_cascade_only_count_via_join(self, schema, mem_conn):
        self._seed(mem_conn)
        counts = get_prune_counts(
            mem_conn, "2024-12-01T00:00:00",
            tables=["workouts", "workout_route"],
        )
        assert counts["workout_route"] == 1  # one old workout → one route row

    def test_no_deletion_occurs(self, schema, mem_conn):
        self._seed(mem_conn)
        get_prune_counts(mem_conn, "2024-12-01T00:00:00", tables=["location_overland"])
        remaining = mem_conn.execute(
            "SELECT COUNT(*) FROM location_overland"
        ).fetchone()[0]
        assert remaining == 3


# ---------------------------------------------------------------------------
# 5. prune_before deletes correct rows; cascade-only returns -1
# ---------------------------------------------------------------------------

class TestPruneBefore:

    def _seed(self, conn):
        conn.execute(
            "INSERT INTO workouts (start_ts, name) VALUES (?, ?), (?, ?)",
            ("2024-01-01T00:00:00", "old", "2025-06-01T00:00:00", "new"),
        )
        old_id = conn.execute(
            "SELECT id FROM workouts WHERE name='old'"
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO workout_route (workout_id) VALUES (?)", (old_id,)
        )
        conn.commit()

    def test_direct_rows_deleted(self, schema, mem_conn):
        self._seed(mem_conn)
        result = prune_before(mem_conn, "2025-01-01T00:00:00", tables=["workouts"])
        assert result["workouts"] == 1
        remaining = mem_conn.execute("SELECT COUNT(*) FROM workouts").fetchone()[0]
        assert remaining == 1

    def test_cascade_only_returns_minus_one(self, schema, mem_conn):
        self._seed(mem_conn)
        result = prune_before(
            mem_conn, "2025-01-01T00:00:00", tables=["workouts", "workout_route"]
        )
        assert result["workout_route"] == -1

    def test_cascade_deletes_child_rows(self, schema, mem_conn):
        self._seed(mem_conn)
        prune_before(mem_conn, "2025-01-01T00:00:00", tables=["workouts", "workout_route"])
        remaining = mem_conn.execute(
            "SELECT COUNT(*) FROM workout_route"
        ).fetchone()[0]
        assert remaining == 0  # cascade removed it

    def test_cutoff_is_exclusive(self, schema, mem_conn):
        mem_conn.execute(
            "INSERT INTO workouts (start_ts, name) VALUES (?, ?)",
            ("2024-06-01T00:00:00", "exact"),
        )
        mem_conn.commit()
        result = prune_before(
            mem_conn, "2024-06-01T00:00:00", tables=["workouts"]
        )
        assert result["workouts"] == 0


# ---------------------------------------------------------------------------
# 6. _build_schema with missing DB file returns empty structures
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# 7. FK cycle handling
# ---------------------------------------------------------------------------

# Schema that mirrors the known_places / place_visits circular FK pair.
# known_places.current_visit_id → place_visits  (soft pointer, no CASCADE)
# place_visits.known_place_id   → known_places   (ON DELETE CASCADE)
# The edge (known_places → place_visits) is in FK_IGNORE, so Kahn's resolves cleanly.
_CIRCULAR_SCHEMA_SQL = """
    CREATE TABLE known_places (
        id               INTEGER PRIMARY KEY,
        first_seen       TEXT NOT NULL,
        current_visit_id INTEGER REFERENCES place_visits(id)
    );

    CREATE TABLE place_visits (
        id             INTEGER PRIMARY KEY,
        arrived_at     TEXT NOT NULL,
        known_place_id INTEGER REFERENCES known_places(id) ON DELETE CASCADE
    );
"""

# Schema with a circular FK pair that is NOT registered in FK_IGNORE.
_UNREGISTERED_CYCLE_SQL = """
    CREATE TABLE alpha (
        id      INTEGER PRIMARY KEY,
        ts      TEXT NOT NULL,
        beta_id INTEGER REFERENCES beta(id)
    );

    CREATE TABLE beta (
        id       INTEGER PRIMARY KEY,
        ts       TEXT NOT NULL,
        alpha_id INTEGER REFERENCES alpha(id)
    );
"""


class TestFKCycles:

    def test_registered_cycle_resolves_place_visits_before_known_places(self, caplog):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(_CIRCULAR_SCHEMA_SQL)

        with caplog.at_level(logging.WARNING, logger="database.pruning"):
            tc, co, do, dt, fk = _build_schema(conn)

        # No warnings should be emitted for the registered cycle
        assert not any("cycle" in r.message.lower() for r in caplog.records)

        # Both tables are directly prunable (not cascade-only)
        assert "place_visits" in tc
        assert "known_places" in tc
        assert "place_visits" not in co
        assert "known_places" not in co

        # place_visits must appear before known_places (FK-safe deletion order)
        assert "place_visits" in do
        assert "known_places" in do
        assert do.index("place_visits") < do.index("known_places")

    def test_unregistered_cycle_raises_runtime_error(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(_UNREGISTERED_CYCLE_SQL)

        with pytest.raises(RuntimeError, match=r"alpha|beta"):
            _build_schema(conn)

    def test_fk_ignore_entry_with_no_matching_db_edge_is_harmless(self, mem_conn):
        # mem_conn has workouts/workout_route/etc but no known_places or place_visits.
        # FK_IGNORE contains ("known_places", "place_visits") — that edge simply never
        # matches, so discovery completes without error.
        tc, co, do, dt, fk = _build_schema(mem_conn)
        assert "workouts" in tc
        assert "location_overland" in tc


class TestMissingDB:

    def test_returns_empty_on_missing_db(self, tmp_path):
        # _build_schema() itself returns empty structures; the module-level
        # constants pick up the hardcoded fallback separately.
        missing = tmp_path / "nonexistent.db"
        with mock.patch("database.pruning.DB_FILE", missing):
            tc, co, do, dt, fk = _build_schema()
        assert tc == {}
        assert co == set()
        assert do == []
        assert dt == []
        assert fk == {}

    def test_does_not_raise_on_missing_db(self, tmp_path):
        missing = tmp_path / "nonexistent.db"
        with mock.patch("database.pruning.DB_FILE", missing):
            try:
                _build_schema()
            except Exception as exc:
                pytest.fail(f"_build_schema() raised unexpectedly: {exc}")

    def test_module_constants_non_empty_even_without_db(self):
        # When DB is absent, module constants are populated from hardcoded fallback.
        import database.pruning as pm
        assert len(pm.TABLE_CONFIG) > 0
        assert len(pm.CASCADE_ONLY) > 0
        assert len(pm.DELETION_ORDER) > 0
