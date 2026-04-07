import json
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException  # type: ignore
from pydantic import BaseModel  # type: ignore

from auth import require_upload_token
from config.general import DATA_DIR

router = APIRouter()
logger = logging.getLogger(__name__)

JOURNAL_LATEST_FILE = DATA_DIR / "journal_latest.json"


class JournalLatestPayload(BaseModel):
    timestamp: str  # ISO 8601, e.g. "2026-04-07T21:30:00"


@router.post("/latest", dependencies=[Depends(require_upload_token)])
async def upload_journal_latest(payload: JournalLatestPayload):
    """Store the timestamp of the most recent journal entry.

    Expects an ISO 8601 timestamp string.  The value is written to a single
    JSON file in the data directory so it can be read by scheduled checks.
    """
    try:
        datetime.fromisoformat(payload.timestamp)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid ISO 8601 timestamp")

    data = {
        "timestamp": payload.timestamp,
        "uploaded_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    with open(JOURNAL_LATEST_FILE, "w") as f:
        json.dump(data, f, indent=2)

    logger.info(f"Journal latest timestamp updated to {payload.timestamp}")
    return {"status": "success", "timestamp": payload.timestamp}
