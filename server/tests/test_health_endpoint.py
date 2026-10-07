# tests/test_health_endpoint.py
#
# /health is the Docker liveness probe. It must answer without auth or a DB, and
# must not be reachable through the public hostnames.

from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

# Prevent module-level logging side effects when importing main
with patch("config.logging.configure_logging"):
    from main import app


@pytest.fixture(autouse=True)
def _mock_lifespan_deps():
    """Suppress all lifespan startup side effects (DB init, notifications, backups)."""
    with patch("main.init_db"), \
         patch("notifications.send_notification"), \
         patch("scheduled_tasks.departure_backup.schedule_departure_backups"):
        yield


client = TestClient(app)


def test_health_returns_ok_without_auth():
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_health_does_not_touch_the_database():
    with patch("database.connection.get_conn", side_effect=AssertionError("DB used")):
        assert client.get("/health").status_code == 200


def test_health_is_hidden_from_openapi_schema():
    assert "/health" not in client.get("/openapi.json").json()["paths"]


@pytest.mark.parametrize("host", ["api.travelnet.dev", "public.travelnet.dev"])
def test_health_is_blocked_on_public_hostnames(host):
    resp = client.get("/health", headers={"host": host})
    assert resp.status_code == 403
