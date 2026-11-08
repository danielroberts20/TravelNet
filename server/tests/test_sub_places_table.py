"""
test_sub_places_table.py — Unit tests for database/location/sub_places/table.py.

Covers:
  init: creates tables, seeds default types, idempotent
  types: get_or_create_type is case-insensitive and reuses ids; list_types grouped
  insert/get: defaults, joined type name
  list_for_place: scoped to parent, hides archived unless asked
  update: partial updates, unknown columns ignored, missing row -> False
  archive_sub_place: soft delete, row retained, second archive -> False
  parent delete cascades to sub-places
"""

import sqlite3
import pytest
from unittest.mock import patch

from database.location.sub_places.table import (
    DEFAULT_RADIUS_M, DEFAULT_TYPES, SubPlaceRecord, SubPlacesTable,
)


@pytest.fixture
def tbl():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("CREATE TABLE known_places (id INTEGER PRIMARY KEY AUTOINCREMENT)")
    conn.execute("INSERT INTO known_places DEFAULT VALUES")
    conn.execute("INSERT INTO known_places DEFAULT VALUES")
    with patch("database.location.sub_places.table.get_conn", return_value=conn):
        t = SubPlacesTable()
        t.init()
        yield t, conn


def test_init_seeds_default_types_and_is_idempotent(tbl):
    t, conn = tbl
    t.init()
    assert conn.execute("SELECT COUNT(*) FROM sub_place_types").fetchone()[0] == len(DEFAULT_TYPES)


def test_get_or_create_type_is_case_insensitive(tbl):
    t, conn = tbl
    existing = conn.execute("SELECT id FROM sub_place_types WHERE name = 'cafe'").fetchone()[0]
    assert t.get_or_create_type("  Cafe ") == existing
    new = t.get_or_create_type("bookshop")
    assert new != existing
    assert t.get_or_create_type("BOOKSHOP") == new


def test_list_types_includes_custom_after_seeded(tbl):
    t, _ = tbl
    t.get_or_create_type("bookshop")
    names = [x["name"] for x in t.list_types()]
    assert "cafe" in names and names[-1] == "bookshop"  # uncategorised sorts last


def test_insert_defaults_and_joined_type(tbl):
    t, _ = tbl
    type_id = t.get_or_create_type("cafe")
    sid = t.insert(SubPlaceRecord(known_place_id=1, name="Joe's", type_id=type_id))
    row = t.get(sid)
    assert row["name"] == "Joe's" and row["type"] == "cafe"
    assert row["radius_m"] == DEFAULT_RADIUS_M
    assert row["latitude"] is None and row["archived_at"] is None
    assert row["created_at"].endswith("Z")


def test_list_for_place_scoped_and_hides_archived(tbl):
    t, _ = tbl
    a = t.insert(SubPlaceRecord(1, "Bravo"))
    b = t.insert(SubPlaceRecord(1, "alpha"))
    t.insert(SubPlaceRecord(2, "other parent"))
    t.archive_sub_place(a)
    assert [r["name"] for r in t.list_for_place(1)] == ["alpha"]
    assert [r["name"] for r in t.list_for_place(1, include_archived=True)] == ["alpha", "Bravo"]


def test_update_partial_and_ignores_unknown_columns(tbl):
    t, _ = tbl
    sid = t.insert(SubPlaceRecord(1, "Old", notes="keep"))
    assert t.update(sid, name="New", known_place_id=2, bogus="x") is True
    row = t.get(sid)
    assert row["name"] == "New" and row["notes"] == "keep" and row["known_place_id"] == 1


def test_update_can_clear_a_field(tbl):
    t, _ = tbl
    sid = t.insert(SubPlaceRecord(1, "X", latitude=1.0, longitude=2.0))
    t.update(sid, latitude=None, longitude=None)
    assert t.get(sid)["latitude"] is None


def test_update_missing_returns_false(tbl):
    t, _ = tbl
    assert t.update(999, name="x") is False
    assert t.update(999) is False


def test_archive_is_soft_and_one_shot(tbl):
    t, conn = tbl
    sid = t.insert(SubPlaceRecord(1, "X"))
    assert t.archive_sub_place(sid) is True
    assert t.archive_sub_place(sid) is False
    assert conn.execute("SELECT archived_at FROM sub_places WHERE id = ?", (sid,)).fetchone()[0]


def test_deleting_parent_cascades(tbl):
    t, conn = tbl
    t.insert(SubPlaceRecord(1, "X"))
    conn.execute("DELETE FROM known_places WHERE id = 1")
    assert conn.execute("SELECT COUNT(*) FROM sub_places").fetchone()[0] == 0
