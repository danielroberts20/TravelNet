"""
middleware.py
~~~~~~~~~~~~~
Custom ASGI middleware for the TravelNet API.

PublicPathFilterMiddleware restricts requests arriving via the public-facing
hostname (api.travelnet.dev) to only the paths listed in PUBLIC_ALLOWED_PREFIXES.
All other paths return 403. Requests via the internal hostname are unaffected.
"""

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from config.general import PUBLIC_ALLOWED_PREFIXES


class PublicPathFilterMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        host = request.headers.get("host", "")
        if "api.travelnet.dev" in host:
            if not any(request.url.path.startswith(p) for p in PUBLIC_ALLOWED_PREFIXES):
                return JSONResponse({"detail": "Forbidden"}, status_code=403)
        return await call_next(request)
