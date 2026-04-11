"""
test_auth.py — Unit tests for auth.py dependency functions.

Covers require_upload_token():
  - Falsy settings.upload_token → request always passes (dev bypass)
  - Correct "Bearer <token>" header → passes
  - Wrong token value → HTTPException(401)
  - Missing header (None) → HTTPException(401)
  - Header without "Bearer" prefix → HTTPException(401)

Covers verify_overland_token():
  - Correct "Bearer <token>" header → passes
  - Wrong token value → HTTPException(401)
  - Non-Bearer scheme → HTTPException(401)
  - Empty token part → HTTPException(401)
"""

import pytest
from unittest.mock import patch, MagicMock
from fastapi import HTTPException

from auth import require_upload_token, verify_overland_token


def _settings(upload_token="", overland_token=""):
    s = MagicMock()
    s.upload_token = upload_token
    s.overland_token = overland_token
    return s


# ---------------------------------------------------------------------------
# require_upload_token
# ---------------------------------------------------------------------------

class TestRequireUploadToken:

    def test_falsy_token_bypasses_check(self):
        """No token configured → any request passes (dev mode)."""
        with patch("auth.settings", _settings(upload_token="")):
            require_upload_token(authorization=None)  # no exception

    def test_none_token_bypasses_check(self):
        with patch("auth.settings", _settings(upload_token=None)):
            require_upload_token(authorization=None)

    def test_correct_bearer_token_passes(self):
        with patch("auth.settings", _settings(upload_token="secret")):
            require_upload_token(authorization="Bearer secret")  # no exception

    def test_wrong_token_raises_401(self):
        with patch("auth.settings", _settings(upload_token="secret")):
            with pytest.raises(HTTPException) as exc_info:
                require_upload_token(authorization="Bearer wrong")
        assert exc_info.value.status_code == 401

    def test_missing_header_raises_401(self):
        with patch("auth.settings", _settings(upload_token="secret")):
            with pytest.raises(HTTPException) as exc_info:
                require_upload_token(authorization=None)
        assert exc_info.value.status_code == 401

    def test_no_bearer_prefix_raises_401(self):
        with patch("auth.settings", _settings(upload_token="secret")):
            with pytest.raises(HTTPException) as exc_info:
                require_upload_token(authorization="secret")  # missing "Bearer "
        assert exc_info.value.status_code == 401

    def test_different_scheme_raises_401(self):
        with patch("auth.settings", _settings(upload_token="secret")):
            with pytest.raises(HTTPException) as exc_info:
                require_upload_token(authorization="Token secret")
        assert exc_info.value.status_code == 401


# ---------------------------------------------------------------------------
# verify_overland_token
# ---------------------------------------------------------------------------

class TestVerifyOverlandToken:

    def test_correct_bearer_token_passes(self):
        with patch("auth.settings", _settings(overland_token="gps-token")):
            verify_overland_token(authorization="Bearer gps-token")  # no exception

    def test_wrong_token_raises_401(self):
        with patch("auth.settings", _settings(overland_token="gps-token")):
            with pytest.raises(HTTPException) as exc_info:
                verify_overland_token(authorization="Bearer wrong")
        assert exc_info.value.status_code == 401

    def test_non_bearer_scheme_raises_401(self):
        with patch("auth.settings", _settings(overland_token="gps-token")):
            with pytest.raises(HTTPException) as exc_info:
                verify_overland_token(authorization="Token gps-token")
        assert exc_info.value.status_code == 401

    def test_empty_token_part_raises_401(self):
        with patch("auth.settings", _settings(overland_token="gps-token")):
            with pytest.raises(HTTPException) as exc_info:
                verify_overland_token(authorization="Bearer ")
        assert exc_info.value.status_code == 401

    def test_case_insensitive_bearer_scheme(self):
        """Scheme matching is case-insensitive (bearer vs Bearer)."""
        with patch("auth.settings", _settings(overland_token="gps-token")):
            verify_overland_token(authorization="bearer gps-token")  # no exception
