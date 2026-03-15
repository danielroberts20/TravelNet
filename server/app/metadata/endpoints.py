from datetime import datetime, timezone
import logging
from fastapi import APIRouter, Header, Query  # type: ignore
from fastapi.responses import Response  # type: ignore

from auth import check_auth
from metadata.util import get_db_stats, get_fx_latest_date, get_last_uploads, get_pending_digest_count, get_uptime, read_last_lines_efficient
from config.general import LOG_FILE


router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/logs")
async def get_logs(lines: int = Query(200, ge=1, le=1000), authorization: str = Header(None)):
    """
    Return the last `lines` lines of the main server logs.
    - `lines`: number of lines to return, default 200, min 1, max 1000
    """
    check_auth(authorization)

    logs = read_last_lines_efficient(LOG_FILE, n=lines)
    return Response(content=logs, media_type="text/plain")

@router.get("/status")
async def get_status():
    return {
        "uptime": get_uptime(),
        "db": get_db_stats(),
        "last_upload": get_last_uploads(),
        "fx_latest_date": get_fx_latest_date(),
        "pending_digest_records": get_pending_digest_count(),
        "generated_at": datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
    }