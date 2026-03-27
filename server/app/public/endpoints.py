"""
public/endpoints.py
~~~~~~~~~~~~~~~~~~~
Public-facing read-only stats endpoint.

- No auth required (data is non-sensitive counts only)
- Rate limited via slowapi
- CORS handled in main.py scoped to this prefix
"""

import logging
from fastapi import APIRouter, Request  # type: ignore
from slowapi import Limiter  # type: ignore
from slowapi.util import get_remote_address  # type: ignore

from public.util import build_public_stats

logger = logging.getLogger(__name__)

limiter = Limiter(key_func=get_remote_address)
router = APIRouter()


@router.get("/stats")
@limiter.limit("30/minute")
async def public_stats(request: Request):
    """
    Returns sanitised trip statistics for the public demo site.
    Contains counts and city-level metadata only — no raw location data.
    """
    return build_public_stats()