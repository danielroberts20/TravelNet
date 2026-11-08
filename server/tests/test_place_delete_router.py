"""
test_place_delete_router.py — HTTP tests for soft-deleting / restoring known places.

Covers: confirm_id guard, response counts, row retained with deleted_at set,
hidden from list/list-null and from every per-place endpoint, double delete,
restore, and that visits of a deleted place leave place_visits_cleaned.
"""

import sqlite3
import pytest
from unittest.mock import patch
from fastapi import FastAPI
from fastapi.testclient import TestClient

from auth import require_upload_token
from upload.places import router as places_router
from database.location.known_places.table import KnownPlacesTable
from database.location.sub_places.table import SubPlacesTable


@pytest.fixture
def env():
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("""CREATE TABLE places (id INTEGER PRIMARY KEY, lat_snap REAL, lon_snap REAL, country TEXT,
                    city TEXT, suburb TEXT, road TEXT, display_name TEXT, geocoded_at TEXT, timezone TEXT)""")
    app = FastAPI()
    app.include_router(places_router.router, prefix="/upload/places")
    app.dependency_overrides[require_upload_token] = lambda: None
    with patch("database.location.known_places.table.get_conn", return_value=conn), \
         patch("database.location.sub_places.table.get_conn", return_value=conn), \
         patch.object(places_router, "get_conn", return_value=conn):
        KnownPlacesTable().init()
        KnownPlacesTable().init_cleaned_view()
        SubPlacesTable().init()
        conn.execute("""INSERT INTO known_places (label, latitude, longitude, first_seen, visit_count, total_time_mins)
                        VALUES ('ERROR MID FLIGHT POINT', 32.1, -104.3, '2026-08-22T01:30:01Z', 1, 90)""")
        conn.execute("INSERT INTO place_visits (known_place_id, arrived_at) VALUES (1, '2026-08-22T01:30:01Z')")
        conn.execute("""INSERT INTO sub_places (known_place_id, name, created_at)
                        VALUES (1, 'x', '2026-08-22T01:30:01Z')""")
        conn.commit()
        yield TestClient(app), conn


def _delete(client, place_id=1, confirm=None):
    return client.request("DELETE", f"/upload/places/{place_id}",
                          json={"confirm_id": place_id if confirm is None else confirm})


def test_delete_returns_counts_and_keeps_row(env):
    client, conn = env
    r = _delete(client)
    assert r.status_code == 200
    d = r.json()
    assert (d["status"], d["id"], d["visits_hidden"], d["sub_places_hidden"]) == ("deleted", 1, 1, 1)
    row = conn.execute("SELECT deleted_at FROM known_places WHERE id = 1").fetchone()
    assert row["deleted_at"] is not None
    assert conn.execute("SELECT COUNT(*) FROM place_visits").fetchone()[0] == 1   # raw data kept


def test_confirm_id_must_match(env):
    client, conn = env
    assert _delete(client, 1, confirm=2).status_code == 400
    assert client.request("DELETE", "/upload/places/1").status_code == 422        # body missing
    assert conn.execute("SELECT deleted_at FROM known_places WHERE id = 1").fetchone()[0] is None


def test_delete_missing_or_twice_404(env):
    client, _ = env
    assert _delete(client, 99).status_code == 404
    assert _delete(client).status_code == 200
    assert _delete(client).status_code == 404


def test_deleted_place_hidden_from_lists_and_endpoints(env):
    client, _ = env
    assert len(client.get("/upload/places/list").json()["places"]) == 1
    _delete(client)
    assert client.get("/upload/places/list").json()["places"] == []
    assert client.get("/upload/places/list-null").json()["places"] == []
    assert client.get("/upload/places/1/visits").status_code == 404
    assert client.get("/upload/places/1/sub-places").status_code == 404
    assert client.patch("/upload/places/1", json={"label": "x"}).status_code == 404
    assert client.post("/upload/places/1/sub-places", json={"name": "y"}).status_code == 404


def test_visits_leave_cleaned_view(env):
    client, conn = env
    assert conn.execute("SELECT COUNT(*) FROM place_visits_cleaned").fetchone()[0] == 1
    _delete(client)
    assert conn.execute("SELECT COUNT(*) FROM place_visits_cleaned").fetchone()[0] == 0


def test_restore(env):
    client, _ = env
    assert client.post("/upload/places/1/restore").status_code == 404   # not deleted
    _delete(client)
    assert client.post("/upload/places/1/restore").json() == {"status": "restored", "id": 1}
    assert len(client.get("/upload/places/list").json()["places"]) == 1
