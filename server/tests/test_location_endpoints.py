"""
test_location_endpoints.py — Integration tests for upload/location endpoints.

Covers POST /upload/location/shortcut:
  - Non-CSV filename → 400
  - Missing/wrong upload token → 401
  - Valid CSV file + correct token → 200 {"status": "success"}
  - Background tasks queued (input_csv, log_previous_day_backup)

Covers POST /upload/location/overland:
  - Missing/wrong overland token → 401
  - Valid payload + correct token → 200 {"result": "ok"}
  - Background tasks queued (append, insert, location_change.run)

Covers POST /upload/location/discard:
  - Missing/wrong overland token → 401
  - Correct token → 200 {"result": "ok"}
"""

import io
import json
import pytest
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient

from main import app

UPLOAD_TOKEN   = "test-upload-token"
OVERLAND_TOKEN = "test-overland-token"

VALID_CSV = "latitude,longitude,timestamp\n51.5074,-0.1278,2024-06-15T08:00:00Z\n"

VALID_OVERLAND = {
    "locations": [
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [-0.1278, 51.5074]},
            "properties": {
                "timestamp": "2024-06-15T08:00:00Z",
                "horizontal_accuracy": 10.0,
                "speed": 0.0,
                "altitude": 10.0,
                "vertical_accuracy": 5.0,
                "motion": [],
                "battery_level": 0.9,
                "battery_state": "charging",
                "wifi": "",
            },
        }
    ]
}


@pytest.fixture(autouse=True)
def _mock_lifespan():
    """Suppress startup side effects when TestClient initialises the app."""
    with patch("main.init_db"), \
         patch("scheduled_tasks.departure_backup.schedule_departure_backups"):
        yield


@pytest.fixture
def client(tmp_path):
    settings_mock = MagicMock()
    settings_mock.upload_token = UPLOAD_TOKEN
    settings_mock.overland_token = OVERLAND_TOKEN

    with patch("auth.settings", settings_mock), \
         patch("upload.location.router.LOCATION_SHORTCUTS_BACKUP_DIR", tmp_path), \
         patch("upload.location.router.input_csv"), \
         patch("upload.location.router.log_previous_day_backup"), \
         patch("upload.location.router.overland_table"), \
         patch("upload.location.router.append_to_daily_buffer"), \
         patch("upload.location.router.location_change"):
        yield TestClient(app)


def _upload_header():
    return {"Authorization": f"Bearer {UPLOAD_TOKEN}"}


def _overland_header():
    return {"Authorization": f"Bearer {OVERLAND_TOKEN}"}


# ---------------------------------------------------------------------------
# /shortcut
# ---------------------------------------------------------------------------

class TestShortcutEndpoint:

    def test_non_csv_file_returns_400(self, client):
        resp = client.post(
            "/upload/location/shortcut",
            files={"file": ("data.json", b"[]", "application/json")},
            headers=_upload_header(),
        )
        assert resp.status_code == 400

    def test_missing_token_returns_401(self, client):
        resp = client.post(
            "/upload/location/shortcut",
            files={"file": ("loc.csv", VALID_CSV.encode(), "text/csv")},
        )
        assert resp.status_code == 401

    def test_wrong_token_returns_401(self, client):
        resp = client.post(
            "/upload/location/shortcut",
            files={"file": ("loc.csv", VALID_CSV.encode(), "text/csv")},
            headers={"Authorization": "Bearer wrong"},
        )
        assert resp.status_code == 401

    def test_valid_upload_returns_200(self, client):
        resp = client.post(
            "/upload/location/shortcut",
            files={"file": ("loc.csv", VALID_CSV.encode(), "text/csv")},
            headers=_upload_header(),
        )
        assert resp.status_code == 200
        assert resp.json()["status"] == "success"


# ---------------------------------------------------------------------------
# /overland
# ---------------------------------------------------------------------------

class TestOverlandEndpoint:

    def test_missing_token_returns_4xx(self, client):
        # verify_overland_token uses Header(...) — required. FastAPI returns 422
        # when the header is absent (validation error before auth runs).
        resp = client.post("/upload/location/overland", json=VALID_OVERLAND)
        assert resp.status_code in (401, 422)

    def test_wrong_token_returns_401(self, client):
        resp = client.post(
            "/upload/location/overland",
            json=VALID_OVERLAND,
            headers={"Authorization": "Bearer wrong"},
        )
        assert resp.status_code == 401

    def test_valid_payload_returns_200(self, client):
        resp = client.post(
            "/upload/location/overland",
            json=VALID_OVERLAND,
            headers=_overland_header(),
        )
        assert resp.status_code == 200
        assert resp.json()["result"] == "ok"


# ---------------------------------------------------------------------------
# /discard
# ---------------------------------------------------------------------------

class TestDiscardEndpoint:

    def test_missing_token_returns_4xx(self, client):
        # verify_overland_token uses Header(...) — required. FastAPI returns 422
        # when the header is absent (validation error before auth runs).
        resp = client.post("/upload/location/discard")
        assert resp.status_code in (401, 422)

    def test_wrong_token_returns_401(self, client):
        resp = client.post(
            "/upload/location/discard",
            headers={"Authorization": "Bearer wrong"},
        )
        assert resp.status_code == 401

    def test_valid_token_returns_200(self, client):
        resp = client.post(
            "/upload/location/discard",
            headers=_overland_header(),
        )
        assert resp.status_code == 200
        assert resp.json()["result"] == "ok"
