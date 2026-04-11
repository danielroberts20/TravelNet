"""
test_pruning.py — Unit tests for database/pruning.py.

Covers:
  - validate_tables: unknown table raises ValueError
  - validate_tables: cascade-only without parent raises ValueError
  - validate_tables: valid list passes
  - _parse_cutoff: datetime → ISO string, valid string → same, invalid string raises
  - prune_before: deletes rows before cutoff, returns counts per table
  - prune_before: cascade-only tables return -1 (not directly deleted)
  - get_prune_counts: returns correct counts without deleting
"""

import sqlite3
import pytest
from datetime import datetime, timezone

from database.pruning import (
    validate_tables,
    _parse_cutoff,
    prune_before,
    get_prune_counts,
    CASCADE_ONLY,
)


# ---------------------------------------------------------------------------
# Minimal DB for pruning tests
# ---------------------------------------------------------------------------

@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript("""
        CREATE TABLE transactions (
            id        TEXT NOT NULL,
            source    TEXT NOT NULL,
            bank      TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            amount    REAL NOT NULL,
            currency  TEXT NOT NULL,
            amount_gbp REAL,
            description TEXT,
            payment_reference TEXT,
            payer TEXT,
            payee TEXT,
            merchant TEXT,
            fees REAL DEFAULT 0.0,
            transaction_type TEXT,
            transaction_detail TEXT,
            state TEXT,
            is_internal INTEGER DEFAULT 0,
            is_interest INTEGER DEFAULT 0,
            running_balance REAL,
            raw TEXT NOT NULL,
            PRIMARY KEY (id, currency, source)
        );

        CREATE TABLE state_of_mind (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            start_ts   TEXT NOT NULL,
            valence    REAL
        );

        CREATE TABLE mood_labels (
            id     INTEGER PRIMARY KEY AUTOINCREMENT,
            som_id INTEGER NOT NULL REFERENCES state_of_mind(id) ON DELETE CASCADE,
            label  TEXT NOT NULL
        );

        CREATE TABLE mood_associations (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            som_id      INTEGER NOT NULL REFERENCES state_of_mind(id) ON DELETE CASCADE,
            association TEXT NOT NULL
        );
    """)
    return conn


def _insert_tx(conn, id, ts):
    conn.execute(
        "INSERT INTO transactions (id, source, bank, timestamp, amount, currency, raw) VALUES (?,?,?,?,?,?,?)",
        (id, "revolut", "Revolut", ts, 10.0, "GBP", "{}"),
    )


def _insert_som(conn, ts):
    return conn.execute(
        "INSERT INTO state_of_mind (start_ts, valence) VALUES (?, ?)", (ts, 0.5)
    ).lastrowid


# ---------------------------------------------------------------------------
# validate_tables
# ---------------------------------------------------------------------------

class TestValidateTables:

    def test_unknown_table_raises(self):
        with pytest.raises(ValueError, match="Unknown table"):
            validate_tables(["not_a_real_table"])

    def test_multiple_unknown_tables_raises(self):
        with pytest.raises(ValueError, match="Unknown table"):
            validate_tables(["transactions", "ghost_table"])

    def test_cascade_without_parent_raises_mood_labels(self):
        with pytest.raises(ValueError, match="state_of_mind"):
            validate_tables(["mood_labels"])

    def test_cascade_without_parent_raises_mood_associations(self):
        with pytest.raises(ValueError, match="state_of_mind"):
            validate_tables(["mood_associations"])

    def test_cascade_with_parent_is_valid(self):
        validate_tables(["state_of_mind", "mood_labels", "mood_associations"])

    def test_valid_single_table_passes(self):
        validate_tables(["transactions"])

    def test_empty_list_passes(self):
        validate_tables([])


# ---------------------------------------------------------------------------
# _parse_cutoff
# ---------------------------------------------------------------------------

class TestParseCutoff:

    def test_datetime_returns_iso_string(self):
        dt = datetime(2024, 6, 15, 9, 0, 0, tzinfo=timezone.utc)
        result = _parse_cutoff(dt)
        assert isinstance(result, str)
        assert "2024-06-15" in result

    def test_valid_string_returned_unchanged(self):
        s = "2024-06-15T09:00:00"
        assert _parse_cutoff(s) == s

    def test_invalid_string_raises(self):
        with pytest.raises(ValueError):
            _parse_cutoff("not-a-date")


# ---------------------------------------------------------------------------
# prune_before
# ---------------------------------------------------------------------------

class TestPruneBefore:

    def test_deletes_rows_before_cutoff(self, db):
        _insert_tx(db, "old", "2024-01-01T00:00:00")
        _insert_tx(db, "new", "2024-12-31T00:00:00")
        db.commit()

        result = prune_before(db, "2024-06-01T00:00:00", tables=["transactions"])
        assert result["transactions"] == 1
        remaining = db.execute("SELECT id FROM transactions").fetchall()
        assert len(remaining) == 1
        assert remaining[0]["id"] == "new"

    def test_cutoff_is_exclusive(self, db):
        """Rows at exactly the cutoff timestamp are NOT deleted (WHERE ts < cutoff)."""
        _insert_tx(db, "at_cutoff", "2024-06-01T00:00:00")
        db.commit()

        result = prune_before(db, "2024-06-01T00:00:00", tables=["transactions"])
        assert result["transactions"] == 0

    def test_cascade_only_returns_minus_one(self, db):
        som_id = _insert_som(db, "2024-01-01T00:00:00")
        db.execute("INSERT INTO mood_labels (som_id, label) VALUES (?, ?)", (som_id, "happy"))
        db.commit()

        result = prune_before(
            db, "2024-06-01T00:00:00",
            tables=["state_of_mind", "mood_labels", "mood_associations"],
        )
        assert result.get("mood_labels") == -1
        assert result.get("mood_associations") == -1

    def test_cascade_rows_deleted_via_parent(self, db):
        """Deleting state_of_mind cascades to mood_labels automatically."""
        som_id = _insert_som(db, "2024-01-01T00:00:00")
        db.execute("INSERT INTO mood_labels (som_id, label) VALUES (?, ?)", (som_id, "happy"))
        db.commit()

        prune_before(db, "2024-06-01T00:00:00", tables=["state_of_mind", "mood_labels"])
        count = db.execute("SELECT COUNT(*) FROM mood_labels").fetchone()[0]
        assert count == 0  # cascaded

    def test_no_rows_to_delete(self, db):
        _insert_tx(db, "future", "2025-01-01T00:00:00")
        db.commit()

        result = prune_before(db, "2024-06-01T00:00:00", tables=["transactions"])
        assert result["transactions"] == 0

    def test_multiple_tables_in_one_call(self, db):
        _insert_tx(db, "old_tx", "2024-01-01T00:00:00")
        _insert_som(db, "2024-01-01T00:00:00")
        db.commit()

        result = prune_before(db, "2024-06-01T00:00:00", tables=["transactions", "state_of_mind"])
        assert result["transactions"] == 1
        assert result["state_of_mind"] == 1


# ---------------------------------------------------------------------------
# get_prune_counts (preview without deletion)
# ---------------------------------------------------------------------------

class TestGetPruneCounts:

    def test_counts_without_deleting(self, db):
        _insert_tx(db, "old", "2024-01-01T00:00:00")
        db.commit()

        counts = get_prune_counts(db, "2024-06-01T00:00:00", tables=["transactions"])
        assert counts["transactions"] == 1

        # Row must still be there
        remaining = db.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
        assert remaining == 1

    def test_counts_zero_for_recent_rows(self, db):
        _insert_tx(db, "new", "2024-12-31T00:00:00")
        db.commit()

        counts = get_prune_counts(db, "2024-06-01T00:00:00", tables=["transactions"])
        assert counts["transactions"] == 0
