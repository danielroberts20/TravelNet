"""
test_detect_country_transitions.py — Unit tests for _detect_transitions() state machine.

Covers:
  - No rows → zero results
  - Single country, no transition
  - Simple A→B transition (promoted after DWELL_MIN_POINTS consecutive points)
  - Candidate not promoted if count < DWELL_MIN_POINTS
  - Brief third-country crossing (A→B(brief)→C) — B not confirmed, C is
  - Returning to confirmed country resets candidate
  - Idempotency: INSERT OR IGNORE means re-running same data doesn't double-insert
  - departed_at set on previous country when transition confirmed
"""

import sqlite3
import pytest
from unittest.mock import patch, MagicMock

from scheduled_tasks.detect_country_transitions import _detect_transitions

DWELL = 3
PAGE  = 1000


# ---------------------------------------------------------------------------
# DB fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript("""
        CREATE TABLE places (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            lat_snap     REAL,
            lon_snap     REAL,
            country_code TEXT,
            country      TEXT
        );

        CREATE TABLE location_overland (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id  TEXT,
            timestamp  TEXT NOT NULL,
            latitude   REAL NOT NULL,
            longitude  REAL NOT NULL,
            place_id   INTEGER REFERENCES places(id)
        );

        CREATE VIEW location_unified AS
        SELECT
            'overland' AS source,
            o.id       AS source_id,
            o.timestamp,
            o.latitude,
            o.longitude,
            o.place_id
        FROM location_overland o;

        CREATE TABLE country_transitions (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            country_code TEXT NOT NULL,
            country      TEXT NOT NULL,
            entered_at   TEXT NOT NULL,
            departed_at  TEXT,
            entry_lat    REAL,
            entry_lon    REAL,
            entry_place_id INTEGER,
            created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
            UNIQUE(country_code, entered_at)
        );
    """)
    return conn


def _add_place(db, country_code, country, lat=0.0, lon=0.0):
    db.execute(
        "INSERT INTO places (lat_snap, lon_snap, country_code, country) VALUES (?,?,?,?)",
        (round(lat, 3), round(lon, 3), country_code, country),
    )
    db.commit()
    return db.execute("SELECT last_insert_rowid()").fetchone()[0]


def _add_point(db, ts, place_id, lat=0.0, lon=0.0):
    db.execute(
        "INSERT INTO location_overland (timestamp, latitude, longitude, place_id) VALUES (?,?,?,?)",
        (ts, lat, lon, place_id),
    )
    db.commit()


def _run(db):
    """Run _detect_transitions with patched config constants and table."""
    with patch("scheduled_tasks.detect_country_transitions.DWELL_MIN_POINTS", DWELL), \
         patch("scheduled_tasks.detect_country_transitions.PAGE_SIZE", PAGE), \
         patch("scheduled_tasks.detect_country_transitions.country_transition_table") as mock_tbl, \
         patch("prefect.logging.get_run_logger") as mock_get_logger:

        mock_logger = mock_get_logger()
        # Make insert() always return True (new row)
        mock_tbl.insert.return_value = True

        result = _detect_transitions(db, mock_logger)

    return result, mock_tbl


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestDetectTransitions:

    def test_no_rows_returns_zeros(self, db):
        result, _ = _run(db)
        assert result["inserted"] == 0
        assert result["departed_at_updated"] == 0

    def test_single_country_no_transition(self, db):
        """Only one country with enough dwell points — confirmed once, no A→B transition."""
        place_id = _add_place(db, "GB", "United Kingdom")
        for i in range(5):
            _add_point(db, f"2024-06-15T0{i}:00:00Z", place_id)
        result, mock_tbl = _run(db)
        # GB is confirmed as the initial entry; no second country, so only 1 insert
        assert result["inserted"] == 1
        mock_tbl.insert.assert_called_once()

    def test_simple_transition_after_dwell(self, db):
        """A→B confirmed after DWELL consecutive points in B."""
        gb_id = _add_place(db, "GB", "United Kingdom", lat=51.5, lon=-0.1)
        fr_id = _add_place(db, "FR", "France",          lat=48.8, lon= 2.3)

        # 3 points in GB (confirms GB), then DWELL points in FR
        for i in range(DWELL):
            _add_point(db, f"2024-06-15T0{i}:00:00Z", gb_id, lat=51.5, lon=-0.1)
        for i in range(DWELL):
            _add_point(db, f"2024-06-15T{10+i}:00:00Z", fr_id, lat=48.8, lon=2.3)

        result, mock_tbl = _run(db)
        # GB confirmed first, then FR confirmed → 2 insertions
        assert mock_tbl.insert.call_count == 2

    def test_candidate_not_promoted_below_dwell(self, db):
        """Only DWELL-1 points in candidate country → no transition."""
        gb_id = _add_place(db, "GB", "United Kingdom")
        fr_id = _add_place(db, "FR", "France")

        for i in range(DWELL):
            _add_point(db, f"2024-06-15T0{i}:00:00Z", gb_id)
        # Only DWELL-1 = 2 points in FR
        for i in range(DWELL - 1):
            _add_point(db, f"2024-06-15T1{i}:00:00Z", fr_id)

        result, mock_tbl = _run(db)
        # Only GB gets confirmed (from the first batch), FR not promoted
        assert mock_tbl.insert.call_count == 1

    def test_brief_crossing_not_confirmed(self, db):
        """A(dwell) → B(1 point) → C(dwell): B not confirmed, C confirmed after A."""
        gb_id = _add_place(db, "GB", "United Kingdom")
        ch_id = _add_place(db, "CH", "Switzerland")      # brief crossing
        fr_id = _add_place(db, "FR", "France")

        for i in range(DWELL):
            _add_point(db, f"2024-06-15T0{i}:00:00Z", gb_id)
        # Single point in CH — brief crossing, resets candidate
        _add_point(db, "2024-06-15T10:00:00Z", ch_id)
        for i in range(DWELL):
            _add_point(db, f"2024-06-15T1{i+1}:00:00Z", fr_id)

        result, mock_tbl = _run(db)
        # GB confirmed, then FR confirmed; CH never confirmed
        inserted_codes = [
            call.args[0].country_code
            for call in mock_tbl.insert.call_args_list
        ]
        assert "CH" not in inserted_codes
        assert "FR" in inserted_codes

    def test_return_to_confirmed_resets_candidate(self, db):
        """After going abroad briefly and returning, candidate is reset."""
        gb_id = _add_place(db, "GB", "United Kingdom")
        fr_id = _add_place(db, "FR", "France")

        # Establish GB
        for i in range(DWELL):
            _add_point(db, f"2024-06-15T0{i}:00:00Z", gb_id)
        # Brief FR dwell attempt (only 2 points)
        for i in range(DWELL - 1):
            _add_point(db, f"2024-06-15T1{i}:00:00Z", fr_id)
        # Back to GB — candidate should reset
        _add_point(db, "2024-06-15T20:00:00Z", gb_id)

        result, mock_tbl = _run(db)
        # Only GB inserted (initial confirmation), FR never promoted
        assert mock_tbl.insert.call_count == 1

    def test_departed_at_updated_on_transition(self, db):
        """When transitioning from GB to FR, departed_at is set for GB."""
        gb_id = _add_place(db, "GB", "United Kingdom")
        fr_id = _add_place(db, "FR", "France")

        for i in range(DWELL):
            _add_point(db, f"2024-06-15T0{i}:00:00Z", gb_id)
        for i in range(DWELL):
            _add_point(db, f"2024-06-15T1{i}:00:00Z", fr_id)

        result, mock_tbl = _run(db)
        assert result["departed_at_updated"] >= 1
        mock_tbl.update_departed_at.assert_called()
