"""
test_photos_table.py — Unit tests for database/photos/table.py.

Covers PhotoMetadataTable.insert():
  - New record returns True
  - Duplicate file_path (UNIQUE constraint) returns False without raising
  - raw_exif dict is JSON-serialised before storage
  - raw_exif string is stored verbatim
  - Optional fields stored as NULL when not provided
  - Integer flags coerced correctly (is_screenshot, is_screen_recording, is_favourite)
"""

import json
import sqlite3
import pytest
from unittest.mock import patch

from database.photos.table import PhotoMetadataTable, PhotoMetadataRecord


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    # photo_metadata has a FK to places(id)
    conn.executescript("""
        CREATE TABLE places (
            id   INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT
        );
    """)
    return conn


@pytest.fixture
def tbl(db):
    """PhotoMetadataTable with get_conn patched to the in-memory DB."""
    with patch("database.photos.table.get_conn", return_value=db):
        t = PhotoMetadataTable()
        t.init()
        yield t, db


def _minimal_record(**kwargs) -> PhotoMetadataRecord:
    """Minimal valid record — only required fields."""
    defaults = dict(
        file_path="/photos/img.jpg",
        taken_at="2024-06-15T10:30:00Z",
        is_screenshot=0,
        is_screen_recording=0,
        is_favourite=0,
    )
    defaults.update(kwargs)
    return PhotoMetadataRecord(**defaults)


# ---------------------------------------------------------------------------
# insert — return value
# ---------------------------------------------------------------------------

class TestInsertReturnValue:

    def test_new_record_returns_true(self, tbl):
        t, db = tbl
        assert t.insert(_minimal_record()) is True

    def test_duplicate_file_path_returns_false(self, tbl):
        t, db = tbl
        t.insert(_minimal_record())
        assert t.insert(_minimal_record()) is False

    def test_duplicate_does_not_raise(self, tbl):
        t, db = tbl
        t.insert(_minimal_record())
        # Should not raise — uses INSERT OR IGNORE
        t.insert(_minimal_record())


# ---------------------------------------------------------------------------
# insert — data stored
# ---------------------------------------------------------------------------

class TestInsertStoredData:

    def test_required_fields_stored(self, tbl):
        t, db = tbl
        t.insert(_minimal_record(file_path="/photos/shot.jpg", taken_at="2024-06-15T10:30:00Z"))
        row = db.execute("SELECT * FROM photo_metadata WHERE file_path = ?", ("/photos/shot.jpg",)).fetchone()
        assert row is not None
        assert row["taken_at"] == "2024-06-15T10:30:00Z"

    def test_optional_fields_null_by_default(self, tbl):
        t, db = tbl
        t.insert(_minimal_record())
        row = db.execute("SELECT * FROM photo_metadata").fetchone()
        assert row["filename"] is None
        assert row["latitude"] is None
        assert row["longitude"] is None
        assert row["place_id"] is None
        assert row["camera_make"] is None
        assert row["raw_exif"] is None

    def test_raw_exif_dict_serialised_to_json_string(self, tbl):
        t, db = tbl
        exif = {"Make": "Apple", "Model": "iPhone 15 Pro"}
        t.insert(_minimal_record(raw_exif=exif))
        row = db.execute("SELECT raw_exif FROM photo_metadata").fetchone()
        assert row["raw_exif"] is not None
        # Must be valid JSON that round-trips back to the original dict
        assert json.loads(row["raw_exif"]) == exif

    def test_raw_exif_string_stored_verbatim(self, tbl):
        t, db = tbl
        exif_str = '{"Make": "Canon"}'
        t.insert(_minimal_record(raw_exif=exif_str))
        row = db.execute("SELECT raw_exif FROM photo_metadata").fetchone()
        assert row["raw_exif"] == exif_str

    def test_is_screenshot_stored_as_integer(self, tbl):
        t, db = tbl
        t.insert(_minimal_record(is_screenshot=1))
        row = db.execute("SELECT is_screenshot FROM photo_metadata").fetchone()
        assert row["is_screenshot"] == 1

    def test_place_id_fk_stored(self, tbl):
        t, db = tbl
        db.execute("INSERT INTO places (name) VALUES ('Home')")
        db.commit()
        t.insert(_minimal_record(place_id=1))
        row = db.execute("SELECT place_id FROM photo_metadata").fetchone()
        assert row["place_id"] == 1

    def test_created_at_auto_populated(self, tbl):
        t, db = tbl
        t.insert(_minimal_record())
        row = db.execute("SELECT created_at FROM photo_metadata").fetchone()
        assert row["created_at"] is not None
        assert "T" in row["created_at"]  # ISO 8601 format


# ---------------------------------------------------------------------------
# init — schema and indices
# ---------------------------------------------------------------------------

class TestInit:

    def test_table_created(self, tbl):
        _, db = tbl
        rows = db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='photo_metadata'"
        ).fetchall()
        assert len(rows) == 1

    def test_indices_created(self, tbl):
        _, db = tbl
        index_names = {
            row["name"]
            for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='photo_metadata'"
            ).fetchall()
        }
        assert "idx_photos_taken_at" in index_names
        assert "idx_photos_local_date" in index_names
        assert "idx_photos_place" in index_names

    def test_init_is_idempotent(self, tbl):
        t, db = tbl
        # Calling init() twice should not raise (CREATE TABLE IF NOT EXISTS)
        with patch("database.photos.table.get_conn", return_value=db):
            t.init()
