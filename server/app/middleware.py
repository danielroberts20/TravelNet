"""
middleware.py
~~~~~~~~~~~~~
Custom ASGI middleware for the TravelNet API.

PublicPathFilterMiddleware enforces hostname-based path allowlists:

  - public.travelnet.dev: only paths in PUBLIC_ALLOWED_PREFIXES pass.
  - api.travelnet.dev: only paths in API_ALLOWED_PREFIXES pass.
    Entries ending in "/" are prefix-matched; others are exact-matched.

Requests via any other hostname (Tailscale, localhost) are unaffected.
"""

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from config.general import API_ALLOWED_PREFIXES, PUBLIC_ALLOWED_PREFIXES


def _api_path_allowed(path: str) -> bool:
    for entry in API_ALLOWED_PREFIXES:
        if entry.endswith("/"):
            if path.startswith(entry):
                return True
        else:
            if path == entry:
                return True
    return False


def get_rate_limit_key(request: Request) -> str:
    """Rate-limit key: prefer CF-Connecting-IP (real client behind Cloudflare), fall back to socket IP."""
    cf_ip = request.headers.get("CF-Connecting-IP")
    if cf_ip:
        return cf_ip
    return request.client.host if request.client else "unknown"


class PublicPathFilterMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        host = request.headers.get("host", "")
        if "public.travelnet.dev" in host:
            if not any(request.url.path.startswith(p) for p in PUBLIC_ALLOWED_PREFIXES):
                return JSONResponse({"detail": "Forbidden"}, status_code=403)
        elif "api.travelnet.dev" in host:
            if not _api_path_allowed(request.url.path):
                return JSONResponse({"detail": "Forbidden"}, status_code=403)
        return await call_next(request)
