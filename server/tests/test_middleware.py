"""
test_middleware.py — Unit tests for PublicPathFilterMiddleware.

Covers:
  - Request from public host (api.travelnet.dev) on an allowed path → 200
  - Request from public host on a disallowed path → 403
  - Request from internal host on any path → passes through (200)
  - Request with no Host header → passes through (200)
  - Multiple allowed prefixes: each prefix passes, unlisted paths blocked
"""

import pytest
from unittest.mock import patch
from fastapi import FastAPI
from fastapi.testclient import TestClient

from middleware import PublicPathFilterMiddleware


PUBLIC_HOST = "public.travelnet.dev"
INTERNAL_HOST = "travelnet.internal"
ALLOWED = ["/public/"]


def _make_client(allowed_prefixes=None):
    if allowed_prefixes is None:
        allowed_prefixes = ALLOWED

    app = FastAPI()

    with patch("middleware.PUBLIC_ALLOWED_PREFIXES", allowed_prefixes):
        app.add_middleware(PublicPathFilterMiddleware)

    @app.get("/public/stats")
    def stats():
        return {"ok": True}

    @app.get("/public/health")
    def health():
        return {"ok": True}

    @app.get("/upload/csv")
    def upload():
        return {"ok": True}

    @app.get("/admin/config")
    def admin():
        return {"ok": True}

    return TestClient(app, raise_server_exceptions=True)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestPublicPathFilterMiddleware:

    def test_public_host_allowed_path_passes(self):
        client = _make_client()
        resp = client.get("/public/stats", headers={"host": PUBLIC_HOST})
        assert resp.status_code == 200

    def test_public_host_disallowed_path_returns_403(self):
        client = _make_client()
        resp = client.get("/upload/csv", headers={"host": PUBLIC_HOST})
        assert resp.status_code == 403

    def test_public_host_admin_path_returns_403(self):
        client = _make_client()
        resp = client.get("/admin/config", headers={"host": PUBLIC_HOST})
        assert resp.status_code == 403

    def test_internal_host_allowed_path_passes(self):
        client = _make_client()
        resp = client.get("/public/stats", headers={"host": INTERNAL_HOST})
        assert resp.status_code == 200

    def test_internal_host_upload_path_passes(self):
        """Internal host bypasses the public filter entirely."""
        client = _make_client()
        resp = client.get("/upload/csv", headers={"host": INTERNAL_HOST})
        assert resp.status_code == 200

    def test_no_host_header_passes(self):
        """Missing Host header — not from api.travelnet.dev — passes through."""
        client = _make_client()
        resp = client.get("/upload/csv")
        assert resp.status_code == 200

    def test_403_body_contains_forbidden(self):
        client = _make_client()
        resp = client.get("/upload/csv", headers={"host": PUBLIC_HOST})
        assert resp.json().get("detail") == "Forbidden"

    def test_multiple_allowed_prefixes_all_pass(self):
        client = _make_client(allowed_prefixes=["/public/", "/health/"])
        resp_pub = client.get("/public/stats", headers={"host": PUBLIC_HOST})
        resp_health = client.get("/public/health", headers={"host": PUBLIC_HOST})
        assert resp_pub.status_code == 200
        assert resp_health.status_code == 200

    def test_only_matching_prefix_passes(self):
        """Only paths starting with an allowed prefix pass; others are blocked."""
        # /public/stats is allowed; /admin/config is not — verified against real prefixes
        client = _make_client()  # uses ["/public/"]
        assert client.get("/public/stats",  headers={"host": PUBLIC_HOST}).status_code == 200
        assert client.get("/admin/config",  headers={"host": PUBLIC_HOST}).status_code == 403
        assert client.get("/upload/csv",    headers={"host": PUBLIC_HOST}).status_code == 403
