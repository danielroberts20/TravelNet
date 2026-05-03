"""
test_known_places_table.py — Unit tests for database/location/known_places/table.py.

Covers:
  - insert: returns lastrowid, sets visit_count=1, last_visited=first_seen
  - label_place: True if updated, False if place_id not found
  - insert_visit: opens a visit row, returns visit_id
  - set_current_visit: sets current_visit_id on known_places row
  - close_visit: sets departed_at/duration_mins, accumulates total_time_mins, clears current_visit_id
  - increment_visit_count: increments counter, sets last_visited and current_visit_id
"""

import sqlite3
import pytest
from unittest.mock import patch

from database.location.known_places.table import KnownPlaceRecord, KnownPlacesTable


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE known_places (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            label            TEXT,
            notes            TEXT,
            latitude         REAL NOT NULL,
            longitude        REAL NOT NULL,
            place_id         INTEGER,
            first_seen       TEXT NOT NULL,
            last_visited     TEXT,
            visit_count      INTEGER NOT NULL DEFAULT 0,
            total_time_mins  INTEGER NOT NULL DEFAULT 0,
            current_visit_id INTEGER REFERENCES place_visits(id)
        );

        CREATE TABLE place_visits (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            known_place_id INTEGER NOT NULL REFERENCES known_places(id),
            arrived_at     TEXT NOT NULL,
            departed_at    TEXT,
            duration_mins  INTEGER
        );
    """)
    return conn


@pytest.fixture
def tbl(db):
    """KnownPlacesTable with get_conn patched to the in-memory DB."""
    with patch("database.location.known_places.table.get_conn", return_value=db):
        yield KnownPlacesTable(), db


# ---------------------------------------------------------------------------
# insert
# ---------------------------------------------------------------------------

class TestInsert:

    def test_returns_integer_id(self, tbl):
        t, db = tbl
        place_id = t.insert(KnownPlaceRecord(latitude=51.5, longitude=-0.1, first_seen="2024-06-15T09:00:00Z"))
        assert isinstance(place_id, int)
        assert place_id > 0

    def test_first_insert_has_id_1(self, tbl):
        t, db = tbl
        place_id = t.insert(KnownPlaceRecord(latitude=51.5, longitude=-0.1, first_seen="2024-06-15T09:00:00Z"))
        assert place_id == 1

    def test_initial_visit_count_is_1(self, tbl):
        t, db = tbl
        t.insert(KnownPlaceRecord(latitude=51.5, longitude=-0.1, first_seen="2024-06-15T09:00:00Z"))
        row = db.execute("SELECT visit_count FROM known_places WHERE id = 1").fetchone()
        assert row["visit_count"] == 1

    def test_last_visited_equals_first_seen(self, tbl):
        t, db = tbl
        t.insert(KnownPlaceRecord(latitude=51.5, longitude=-0.1, first_seen="2024-06-15T09:00:00Z"))
        row = db.execute("SELECT last_visited, first_seen FROM known_places WHERE id = 1").fetchone()
        assert row["last_visited"] == row["first_seen"]

    def test_optional_label_stored(self, tbl):
        t, db = tbl
        t.insert(KnownPlaceRecord(latitude=51.5, longitude=-0.1, first_seen="2024-06-15T09:00:00Z", label="Home"))
        row = db.execute("SELECT label FROM known_places WHERE id = 1").fetchone()
        assert row["label"] == "Home"

    def test_null_label_by_default(self, tbl):
        t, db = tbl
        t.insert(KnownPlaceRecord(latitude=51.5, longitude=-0.1, first_seen="2024-06-15T09:00:00Z"))
        row = db.execute("SELECT label FROM known_places WHERE id = 1").fetchone()
        assert row["label"] is None


# ---------------------------------------------------------------------------
# label_place
# ---------------------------------------------------------------------------

class TestLabelPlace:

    def test_returns_true_when_found(self, tbl):
        t, db = tbl
        place_id = t.insert(KnownPlaceRecord(latitude=51.5, longitude=-0.1, first_seen="2024-06-15T09:00:00Z"))
        result = t.label_place(place_id, "Office")
        assert result is True

    def test_label_stored_correctly(self, tbl):
        t, db = tbl
        place_id = t.insert(KnownPlaceRecord(latitude=51.5, longitude=-0.1, first_seen="2024-06-15T09:00:00Z"))
        t.label_place(place_id, "Office")
        row = db.execute("SELECT label FROM known_places WHERE id = ?", (place_id,)).fetchone()
        assert row["label"] == "Office"

    def test_returns_false_when_not_found(self, tbl):
        t, db = tbl
        result = t.label_place(999, "Ghost")
        assert result is False


# ---------------------------------------------------------------------------
# insert_visit
# ---------------------------------------------------------------------------

class TestInsertVisit:

    def test_returns_visit_id(self, tbl):
        t, db = tbl
        place_id = t.insert(KnownPlaceRecord(latitude=51.5, longitude=-0.1, first_seen="2024-06-15T09:00:00Z"))
        visit_id = t.insert_visit(place_id, "2024-06-15T09:00:00Z")
        assert isinstance(visit_id, int)
        assert visit_id > 0

    def test_visit_row_has_correct_place_id(self, tbl):
        t, db = tbl
        place_id = t.insert(KnownPlaceRecord(latitude=51.5, longitude=-0.1, first_seen="2024-06-15T09:00:00Z"))
        visit_id = t.insert_visit(place_id, "2024-06-15T09:00:00Z")
        row = db.execute("SELECT known_place_id, arrived_at FROM place_visits WHERE id = ?", (visit_id,)).fetchone()
        assert row["known_place_id"] == place_id
        assert row["arrived_at"] == "2024-06-15T09:00:00Z"

    def test_departed_at_initially_null(self, tbl):
        t, db = tbl
        place_id = t.insert(KnownPlaceRecord(latitude=51.5, longitude=-0.1, first_seen="2024-06-15T09:00:00Z"))
        visit_id = t.insert_visit(place_id, "2024-06-15T09:00:00Z")
        row = db.execute("SELECT departed_at FROM place_visits WHERE id = ?", (visit_id,)).fetchone()
        assert row["departed_at"] is None


# ---------------------------------------------------------------------------
# set_current_visit
# ---------------------------------------------------------------------------

class TestSetCurrentVisit:

    def test_sets_current_visit_id(self, tbl):
        t, db = tbl
        place_id = t.insert(KnownPlaceRecord(latitude=51.5, longitude=-0.1, first_seen="2024-06-15T09:00:00Z"))
        visit_id = t.insert_visit(place_id, "2024-06-15T09:00:00Z")
        t.set_current_visit(place_id, visit_id)
        row = db.execute("SELECT current_visit_id FROM known_places WHERE id = ?", (place_id,)).fetchone()
        assert row["current_visit_id"] == visit_id


# ---------------------------------------------------------------------------
# close_visit
# ---------------------------------------------------------------------------

class TestCloseVisit:

    def _setup(self, tbl):
        t, db = tbl
        place_id = t.insert(KnownPlaceRecord(latitude=51.5, longitude=-0.1, first_seen="2024-06-15T09:00:00Z"))
        visit_id = t.insert_visit(place_id, "2024-06-15T09:00:00Z")
        t.set_current_visit(place_id, visit_id)
        return t, db, place_id, visit_id

    def test_sets_departed_at(self, tbl):
        t, db, place_id, visit_id = self._setup(tbl)
        t.close_visit(visit_id, place_id, "2024-06-15T11:00:00Z", 120)
        row = db.execute("SELECT departed_at FROM place_visits WHERE id = ?", (visit_id,)).fetchone()
        assert row["departed_at"] == "2024-06-15T11:00:00Z"

    def test_sets_duration_mins(self, tbl):
        t, db, place_id, visit_id = self._setup(tbl)
        t.close_visit(visit_id, place_id, "2024-06-15T11:00:00Z", 120)
        row = db.execute("SELECT duration_mins FROM place_visits WHERE id = ?", (visit_id,)).fetchone()
        assert row["duration_mins"] == 120

    def test_accumulates_total_time_mins(self, tbl):
        t, db, place_id, visit_id = self._setup(tbl)
        t.close_visit(visit_id, place_id, "2024-06-15T11:00:00Z", 90)
        row = db.execute("SELECT total_time_mins FROM known_places WHERE id = ?", (place_id,)).fetchone()
        assert row["total_time_mins"] == 90

    def test_accumulates_across_multiple_visits(self, tbl):
        t, db, place_id, visit_id = self._setup(tbl)
        t.close_visit(visit_id, place_id, "2024-06-15T11:00:00Z", 90)
        visit_id2 = t.insert_visit(place_id, "2024-06-16T09:00:00Z")
        t.set_current_visit(place_id, visit_id2)
        t.close_visit(visit_id2, place_id, "2024-06-16T11:30:00Z", 150)
        row = db.execute("SELECT total_time_mins FROM known_places WHERE id = ?", (place_id,)).fetchone()
        assert row["total_time_mins"] == 240

    def test_clears_current_visit_id(self, tbl):
        t, db, place_id, visit_id = self._setup(tbl)
        t.close_visit(visit_id, place_id, "2024-06-15T11:00:00Z", 120)
        row = db.execute("SELECT current_visit_id FROM known_places WHERE id = ?", (place_id,)).fetchone()
        assert row["current_visit_id"] is None


# ---------------------------------------------------------------------------
# increment_visit_count
# ---------------------------------------------------------------------------

class TestIncrementVisitCount:

    def test_increments_visit_count(self, tbl):
        t, db = tbl
        place_id = t.insert(KnownPlaceRecord(latitude=51.5, longitude=-0.1, first_seen="2024-06-15T09:00:00Z"))
        visit_id = t.insert_visit(place_id, "2024-06-16T09:00:00Z")
        t.increment_visit_count(place_id, "2024-06-16T09:00:00Z", visit_id)
        row = db.execute("SELECT visit_count FROM known_places WHERE id = ?", (place_id,)).fetchone()
        assert row["visit_count"] == 2  # was 1 after insert, now 2

    def test_sets_last_visited(self, tbl):
        t, db = tbl
        place_id = t.insert(KnownPlaceRecord(latitude=51.5, longitude=-0.1, first_seen="2024-06-15T09:00:00Z"))
        visit_id = t.insert_visit(place_id, "2024-06-16T09:00:00Z")
        t.increment_visit_count(place_id, "2024-06-16T09:00:00Z", visit_id)
        row = db.execute("SELECT last_visited FROM known_places WHERE id = ?", (place_id,)).fetchone()
        assert row["last_visited"] == "2024-06-16T09:00:00Z"

    def test_sets_current_visit_id(self, tbl):
        t, db = tbl
        place_id = t.insert(KnownPlaceRecord(latitude=51.5, longitude=-0.1, first_seen="2024-06-15T09:00:00Z"))
        visit_id = t.insert_visit(place_id, "2024-06-16T09:00:00Z")
        t.increment_visit_count(place_id, "2024-06-16T09:00:00Z", visit_id)
        row = db.execute("SELECT current_visit_id FROM known_places WHERE id = ?", (place_id,)).fetchone()
        assert row["current_visit_id"] == visit_id
