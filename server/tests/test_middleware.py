"""
test_middleware.py — Unit tests for PublicPathFilterMiddleware and get_rate_limit_key.

Covers PublicPathFilterMiddleware:
  - public.travelnet.dev: allowed paths pass, others return 403
  - api.travelnet.dev: /upload/ prefix passes, exact metadata paths pass, others 403
  - Internal/no-host requests are unaffected
  - Multiple allowed prefixes all pass

Covers get_rate_limit_key:
  - CF-Connecting-IP header takes precedence over socket IP
  - Falls back to request.client.host when header absent or empty
"""

import pytest
from unittest.mock import patch, MagicMock
from fastapi import FastAPI
from fastapi.testclient import TestClient

from middleware import PublicPathFilterMiddleware, get_rate_limit_key


PUBLIC_HOST = "public.travelnet.dev"
API_HOST = "api.travelnet.dev"
INTERNAL_HOST = "travelnet.internal"
ALLOWED = ["/public/"]
API_ALLOWED = ["/upload/", "/metadata/status", "/metadata/deployment_tz"]


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


def _make_api_client(api_allowed=None):
    if api_allowed is None:
        api_allowed = API_ALLOWED

    app = FastAPI()

    with patch("middleware.API_ALLOWED_PREFIXES", api_allowed):
        app.add_middleware(PublicPathFilterMiddleware)

    @app.get("/upload/location")
    def upload():
        return {"ok": True}

    @app.get("/metadata/status")
    def status():
        return {"ok": True}

    @app.get("/metadata/deployment_tz")
    def tz():
        return {"ok": True}

    @app.get("/database/download")
    def db():
        return {"ok": True}

    @app.get("/metadata/logs")
    def logs():
        return {"ok": True}

    @app.get("/compute/run")
    def compute():
        return {"ok": True}

    @app.get("/public/stats")
    def public_stats():
        return {"ok": True}

    return TestClient(app, raise_server_exceptions=True)


def _mock_request(cf_ip=None, client_host="127.0.0.1"):
    req = MagicMock()
    req.headers = {"CF-Connecting-IP": cf_ip} if cf_ip else {}
    req.client = MagicMock()
    req.client.host = client_host
    return req


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


# ---------------------------------------------------------------------------
# api.travelnet.dev path filter
# ---------------------------------------------------------------------------

class TestApiHostPathFilter:

    def test_upload_prefix_passes(self):
        client = _make_api_client()
        resp = client.get("/upload/location", headers={"host": API_HOST})
        assert resp.status_code == 200

    def test_metadata_status_exact_passes(self):
        client = _make_api_client()
        resp = client.get("/metadata/status", headers={"host": API_HOST})
        assert resp.status_code == 200

    def test_metadata_deployment_tz_exact_passes(self):
        client = _make_api_client()
        resp = client.get("/metadata/deployment_tz", headers={"host": API_HOST})
        assert resp.status_code == 200

    def test_database_download_returns_403(self):
        client = _make_api_client()
        resp = client.get("/database/download", headers={"host": API_HOST})
        assert resp.status_code == 403

    def test_metadata_logs_returns_403(self):
        client = _make_api_client()
        resp = client.get("/metadata/logs", headers={"host": API_HOST})
        assert resp.status_code == 403

    def test_compute_returns_403(self):
        client = _make_api_client()
        resp = client.get("/compute/run", headers={"host": API_HOST})
        assert resp.status_code == 403

    def test_403_body_contains_forbidden(self):
        client = _make_api_client()
        resp = client.get("/database/download", headers={"host": API_HOST})
        assert resp.json().get("detail") == "Forbidden"

    def test_internal_host_bypasses_api_filter(self):
        """Tailscale/internal host ignores the api.travelnet.dev filter."""
        client = _make_api_client()
        resp = client.get("/database/download", headers={"host": INTERNAL_HOST})
        assert resp.status_code == 200

    def test_no_host_bypasses_api_filter(self):
        client = _make_api_client()
        resp = client.get("/database/download")
        assert resp.status_code == 200

    def test_public_host_still_blocks_non_public_paths(self):
        """public.travelnet.dev behaviour is unchanged after api.travelnet.dev logic was added."""
        client = _make_api_client()
        resp = client.get("/upload/location", headers={"host": PUBLIC_HOST})
        assert resp.status_code == 403

    def test_public_host_still_allows_public_path(self):
        client = _make_api_client()
        resp = client.get("/public/stats", headers={"host": PUBLIC_HOST})
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# get_rate_limit_key
# ---------------------------------------------------------------------------

class TestGetRateLimitKey:

    def test_cf_connecting_ip_takes_precedence(self):
        req = _mock_request(cf_ip="1.2.3.4", client_host="10.0.0.1")
        assert get_rate_limit_key(req) == "1.2.3.4"

    def test_falls_back_to_client_host_when_header_absent(self):
        req = _mock_request(cf_ip=None, client_host="10.0.0.1")
        assert get_rate_limit_key(req) == "10.0.0.1"

    def test_empty_cf_header_falls_back_to_client_host(self):
        """An empty CF-Connecting-IP header is treated as absent."""
        req = _mock_request(cf_ip="", client_host="10.0.0.1")
        assert get_rate_limit_key(req) == "10.0.0.1"

    def test_no_client_falls_back_to_unknown(self):
        req = MagicMock()
        req.headers = {}
        req.client = None
        assert get_rate_limit_key(req) == "unknown"
