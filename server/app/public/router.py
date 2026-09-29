"""
public/router.py
~~~~~~~~~~~~~~~~
Public-facing read-only stats endpoint.

- No auth required (data is non-sensitive counts only)
- Rate limited via slowapi
- CORS handled in main.py scoped to this prefix
"""

import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Request  # type: ignore
from slowapi import Limiter  # type: ignore
from slowapi.util import get_remote_address  # type: ignore

from public.stats import build_public_stats
from public.widget import build_widget_data
from public.location import get_latest_location, get_location_history

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

@router.get("/widget")
@limiter.limit("30/minute")
async def public_widget(request: Request):
    """
    Public widget endpoint for the Scriptable iOS home screen widget.
    No auth. Returns system health and current trip context.
    """
    return build_widget_data()


@router.get("/location")
@limiter.limit("30/minute")
async def public_location(request: Request, since: Optional[str] = Query(None)):
    """
    Fuzzed location for external consumers (e.g. Constellation's
    "how far apart are we" stat). No auth. Coordinates are always randomly
    jittered — never raw — and no other metadata is exposed.

    Without `since`: latest point as {latitude, longitude, last_synced}.
    With `since` (ISO date or datetime): a time-ordered, downsampled array
    of {latitude, longitude, timestamp} points from that time forward.
    """
    if since is not None:
        try:
            since_dt = datetime.fromisoformat(since)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid 'since' — expected ISO date or datetime")
        if since_dt.tzinfo is None:
            since_dt = since_dt.replace(tzinfo=timezone.utc)
        return get_location_history(since_dt)

    location = get_latest_location()
    if location is None:
        raise HTTPException(status_code=404, detail="No location data available")
    return location

