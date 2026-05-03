import io
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
import zipfile

from fastapi import APIRouter, BackgroundTasks, Depends, File, HTTPException, UploadFile  # type: ignore
import httpx
from pydantic import BaseModel  # type: ignore

from config.settings import settings
from auth import require_upload_token
from config.general import DATA_DIR, JOURNAL_BACKUP_DIR

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


def _validate_journal_zip(data: bytes) -> zipfile.ZipFile:
    try:
        zf = zipfile.ZipFile(io.BytesIO(data))
    except zipfile.BadZipFile:
        raise HTTPException(status_code=400, detail="Invalid zip file")

    for name in zf.namelist():
        p = Path(name)
        if name.endswith("/"):
            continue
        # Accept structured layout (Entries/*.html, Resources/*.json)
        # or flat layout (*.html, *.json) from Shortcuts
        if p.suffix in (".html", ".json"):
            continue
        raise HTTPException(
            status_code=400,
            detail=f"Unexpected file in zip: {name}. Only .html and .json files accepted."
        )

    return zf


async def _trigger_trevor_ingestion(zip_path: Path) -> None:
    """
    Notify Trevor to ingest the journal zip at the given path.
    Trevor reads from the shared /data volume — the path is the container-side path.
    Fires and forgets: logs errors but does not raise.
    """
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{settings.trevor_url}/ingest/journal",
                json={"zip_path": str(zip_path)},
                headers={"x-api-key": settings.trevor_api_key},
                timeout=10,
            )
        if resp.status_code == 200:
            logger.info("Trevor ingestion triggered successfully")
        else:
            logger.warning(
                "Trevor ingestion returned unexpected status %d: %s",
                resp.status_code, resp.text
            )
    except Exception as e:
        logger.error("Failed to trigger Trevor ingestion: %s", e)


@router.post("/export", dependencies=[Depends(require_upload_token)])
async def upload_journal_export(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
):
    """
    Accept a filtered Apple Journal export zip (Entries/*.html + Resources/*.json).
    Saves to /data/backups/uploads/journal/ and triggers Trevor ingestion.
    
    The zip must contain only:
      - Entries/*.html  — journal entry HTML files
      - Resources/*.json — asset sidecar JSON files (mood, location metadata)
    
    HEIC/mp4/gif media assets must be excluded before upload (filter in Shortcut).
    """
    data = await file.read()

    if not data:
        raise HTTPException(status_code=400, detail="Empty file")

    _validate_journal_zip(data)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    zip_path = JOURNAL_BACKUP_DIR / f"journal_{timestamp}.zip"
    zip_path.write_bytes(data)

    logger.info(
        "Journal export saved: %s (%.1f KB)",
        zip_path.name, len(data) / 1024
    )

    background_tasks.add_task(_trigger_trevor_ingestion, zip_path)

    return {
        "status": "success",
        "filename": zip_path.name,
        "size_bytes": len(data),
    }