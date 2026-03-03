import logging
from fastapi import APIRouter, Header, Query  # type: ignore
from fastapi.responses import Response  # type: ignore

from auth import check_auth
from metadata.util import read_last_lines_efficient
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