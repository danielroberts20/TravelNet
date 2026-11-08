"""
test_sub_places_router.py — HTTP tests for the sub-place endpoints in upload/places/router.py.

Covers: list/create/update/archive, free-entry type creation, validation
(name, lat/lon pairing, ranges, radius), 404s, and the field-clearing semantics of PATCH.
"""

import sqlite3
import pytest
from unittest.mock import patch
from fastapi import FastAPI
from fastapi.testclient import TestClient

from auth import require_upload_token
from upload.places import router as places_router
from database.location.sub_places.table import SubPlacesTable


@pytest.fixture
def client():
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("CREATE TABLE known_places (id INTEGER PRIMARY KEY AUTOINCREMENT)")
    conn.execute("INSERT INTO known_places DEFAULT VALUES")
    app = FastAPI()
    app.include_router(places_router.router, prefix="/upload/places")
    app.dependency_overrides[require_upload_token] = lambda: None
    with patch("database.location.sub_places.table.get_conn", return_value=conn), \
         patch.object(places_router, "get_conn", return_value=conn):
        SubPlacesTable().init()
        yield TestClient(app)


def _create(client, **body):
    body.setdefault("name", "Joe's")
    return client.post("/upload/places/1/sub-places", json=body)


def test_create_and_list(client):
    r = _create(client, type="cafe", notes="  good wifi ")
    assert r.status_code == 200
    d = r.json()
    assert d["type"] == "cafe" and d["notes"] == "good wifi" and d["radius_m"] == 30
    listing = client.get("/upload/places/1/sub-places").json()
    assert listing["sub_places"][0]["id"] == d["id"]
    assert listing["parent_radius_m"] > 0


def test_free_entry_type_is_created_once_and_listed(client):
    _create(client, name="A", type="Bookshop")
    _create(client, name="B", type="bookshop")
    types = client.get("/upload/places/sub-place-types").json()["types"]
    assert [t["name"] for t in types].count("Bookshop") == 1


def test_create_unknown_parent_404(client):
    assert client.post("/upload/places/99/sub-places", json={"name": "x"}).status_code == 404
    assert client.get("/upload/places/99/sub-places").status_code == 404


@pytest.mark.parametrize("body", [
    {"name": "   "},
    {"latitude": 10.0},
    {"longitude": 10.0},
    {"latitude": 95.0, "longitude": 0.0},
    {"radius_m": 2},
    {"radius_m": 5000},
])
def test_create_validation(client, body):
    assert _create(client, **body).status_code == 422


def test_patch_updates_only_sent_fields_and_clears_with_null(client):
    sid = _create(client, type="cafe", notes="n", latitude=1.0, longitude=2.0).json()["id"]
    r = client.patch(f"/upload/places/sub-places/{sid}", json={"name": "Renamed", "notes": None})
    d = r.json()
    assert d["name"] == "Renamed" and d["notes"] is None
    assert d["type"] == "cafe" and d["latitude"] == 1.0


def test_patch_can_clear_coordinates(client):
    sid = _create(client, latitude=1.0, longitude=2.0).json()["id"]
    d = client.patch(f"/upload/places/sub-places/{sid}", json={"latitude": None, "longitude": None}).json()
    assert d["latitude"] is None and d["longitude"] is None


def test_patch_rejects_half_a_coordinate_pair(client):
    sid = _create(client).json()["id"]
    assert client.patch(f"/upload/places/sub-places/{sid}", json={"latitude": 1.0}).status_code == 422


def test_patch_rejects_empty_name_and_null_radius(client):
    sid = _create(client).json()["id"]
    assert client.patch(f"/upload/places/sub-places/{sid}", json={"name": ""}).status_code == 422
    assert client.patch(f"/upload/places/sub-places/{sid}", json={"radius_m": None}).status_code == 422


def test_patch_missing_404(client):
    assert client.patch("/upload/places/sub-places/999", json={"name": "x"}).status_code == 404


def test_delete_archives_and_hides(client):
    sid = _create(client).json()["id"]
    assert client.delete(f"/upload/places/sub-places/{sid}").json() == {"status": "archived"}
    assert client.get("/upload/places/1/sub-places").json()["sub_places"] == []
    assert client.delete(f"/upload/places/sub-places/{sid}").status_code == 404
    assert client.patch(f"/upload/places/sub-places/{sid}", json={"name": "x"}).status_code == 404
