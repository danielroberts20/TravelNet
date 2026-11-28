import hashlib
import io
import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional
import zipfile

from fastapi import APIRouter, BackgroundTasks, Depends, File, HTTPException, UploadFile  # type: ignore
import httpx
from pydantic import BaseModel, Field, field_validator  # type: ignore

from config.settings import settings
from auth import require_upload_token
from config.general import DATA_DIR, JOURNAL_DIR
from database.connection import get_conn
from database.journal.enrich import enrich, latest_timezone
from database.journal.sync import push_entry_ids
from database.journal.table import JournalEntryRecord, insert_entry

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


class QuickEntryPayload(BaseModel):
    """One quick journal capture (iOS Shortcut)."""
    text: str = Field(min_length=1, max_length=20000)
    timestamp: Optional[str] = None      # ISO 8601; defaults to now. Naive = current local timezone.
    covers_date: Optional[str] = None    # YYYY-MM-DD; set when writing about an earlier day (retro)
    mood: Optional[int] = Field(default=None, ge=1, le=5)         # 1-5 tap
    rating: Optional[int] = Field(default=None, ge=1, le=10)      # day rating
    energy: Optional[int] = Field(default=None, ge=1, le=5)
    tags: Optional[str] = None           # comma-separated
    client_id: Optional[str] = Field(default=None, max_length=100)  # makes retries idempotent

    @field_validator("mood", "rating", "energy", mode="before")
    @classmethod
    def _blank_number_is_unset(cls, v):
        # Shortcuts' JSON builder sends "" (or 0) for a skipped optional field.
        return None if v in ("", 0, "0", None) or (isinstance(v, str) and not v.strip()) else v

    @field_validator("timestamp", "covers_date", "tags", "client_id", mode="before")
    @classmethod
    def _blank_text_is_unset(cls, v):
        return None if isinstance(v, str) and not v.strip() else v


def build_quick_entry(conn, payload: QuickEntryPayload, now: Optional[datetime] = None) -> JournalEntryRecord:
    """Turn a payload into a record, enriching from TravelNet's own data.

    Raises ValueError for bad timestamps/dates (mapped to HTTP 400 by the endpoint).
    """
    now = now or datetime.now(timezone.utc)
    if payload.timestamp:
        ts = datetime.fromisoformat(payload.timestamp.replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=latest_timezone(conn))
        ts_utc = ts.astimezone(timezone.utc)
    else:
        ts_utc = now

    ctx = enrich(conn, ts_utc)
    local_date = ctx["local_date"]

    covers = None
    if payload.covers_date:
        covers = date.fromisoformat(payload.covers_date)
        if covers > date.fromisoformat(local_date):
            raise ValueError("covers_date cannot be in the future")
    retro = covers is not None and covers < date.fromisoformat(local_date)

    if retro:
        # Location/weather/steps would describe when it was *written*, not the day it covers.
        ctx = {k: ctx[k] for k in ("tz", "local_date", "utc_offset")}
    loc = ctx.get("location") or {}

    text = payload.text.strip()
    ts_str = ts_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    covers_str = covers.isoformat() if covers else local_date
    return JournalEntryRecord(
        id=payload.client_id or hashlib.sha256(f"{ts_str}|{text}".encode()).hexdigest()[:20],
        ts_utc=ts_str,
        local_date=local_date,
        text=text,
        source="retro" if retro else "quick",
        provenance="retro" if retro else "contemporaneous",
        tz=ctx["tz"],
        utc_offset=ctx["utc_offset"],
        covers_start=covers_str,
        covers_end=covers_str,
        lag_days=(date.fromisoformat(local_date) - covers).days if retro else None,
        rating=payload.rating,
        energy=payload.energy,
        tags=payload.tags.strip() if payload.tags and payload.tags.strip() else None,
        mood_self=payload.mood,
        valence_self=(payload.mood - 3) / 2 if payload.mood else None,
        latitude=loc.get("latitude"),
        longitude=loc.get("longitude"),
        place_id=loc.get("place_id"),
        enrichment_json=json.dumps({k: v for k, v in ctx.items() if k not in ("tz", "local_date", "utc_offset")}) ,
    )


def _after_save(ids: list[str], days: list[Optional[str]]) -> None:
    """Background work after entries are stored: update daily_summary, then push to Trevor.

    Neither step may fail the request, and a failed push is retried by the nightly reconcile.
    """
    from scheduled_tasks.daily_summary.journal import refresh_journal_days
    try:
        refresh_journal_days(d for d in days if d)
    except Exception as e:
        logger.error("daily_summary journal refresh failed: %s", e)
    push_entry_ids(ids)


class ImportEntry(BaseModel):
    """An already-parsed Apple Journal entry (parsed by Trevor, which owns the HTML parser)."""
    import_key: str = Field(min_length=1, max_length=200)   # source filename stem
    ts_utc: str                                              # ISO UTC, Z suffix
    local_date: str                                          # YYYY-MM-DD in the writer's timezone
    utc_offset: Optional[str] = None
    text: str = Field(min_length=1, max_length=50000)
    rating: Optional[int] = None     # out-of-range values are kept in enrichment, not rejected
    energy: Optional[int] = None
    tags: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    enrichment: dict = Field(default_factory=dict)           # location/weather/mood as exported


class ImportPayload(BaseModel):
    entries: list[ImportEntry] = Field(max_length=2000)


@router.post("/import", dependencies=[Depends(require_upload_token)])
def upload_journal_import(payload: ImportPayload, background_tasks: BackgroundTasks):
    """Import parsed Apple Journal entries into journal_entries (idempotent on import_key)."""
    inserted, ids, days = 0, [], []
    with get_conn() as conn:
        for e in payload.entries:
            try:
                datetime.fromisoformat(e.ts_utc.replace("Z", "+00:00"))
                date.fromisoformat(e.local_date)
            except ValueError:
                raise HTTPException(status_code=400, detail=f"Bad timestamp/date for {e.import_key}")
            # Old template scales were not enforced (e.g. energy 8 on a 1-5 field). Keep
            # the raw value in enrichment and leave the constrained column empty.
            enrichment = dict(e.enrichment)
            rating, energy = e.rating, e.energy
            if rating is not None and not 1 <= rating <= 10:
                enrichment["rating_raw"], rating = rating, None
            if energy is not None and not 1 <= energy <= 5:
                enrichment["energy_raw"], energy = energy, None
            rec = JournalEntryRecord(
                id=f"apple:{e.import_key}",
                ts_utc=e.ts_utc, local_date=e.local_date, utc_offset=e.utc_offset,
                text=e.text.strip(), source="import", provenance="contemporaneous",
                covers_start=e.local_date, covers_end=e.local_date,
                rating=rating, energy=energy, tags=e.tags,
                latitude=e.latitude, longitude=e.longitude,
                enrichment_json=json.dumps(enrichment), import_key=e.import_key,
            )
            if insert_entry(conn, rec):
                inserted += 1
                ids.append(rec.id)
                days.append(rec.local_date)

    if ids:
        background_tasks.add_task(_after_save, ids, days)
    logger.info("Journal import: %d received, %d inserted", len(payload.entries), inserted)
    return {"status": "success", "received": len(payload.entries),
            "inserted": inserted, "skipped": len(payload.entries) - inserted}


@router.post("/quick", dependencies=[Depends(require_upload_token)])
def upload_journal_quick(payload: QuickEntryPayload, background_tasks: BackgroundTasks):
    """Quick journal capture. Stored in journal_entries, enriched from TravelNet's own data.

    Synchronous on purpose (unlike most uploads): the Shortcut should only report
    success once the entry is actually stored, since this is the user's own words.
    Resending the same client_id (or identical timestamp+text) is a no-op.
    """
    with get_conn() as conn:
        try:
            record = build_quick_entry(conn, payload)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        added = insert_entry(conn, record)

    if added:
        background_tasks.add_task(_after_save, [record.id], [record.covers_start, record.covers_end])

    if added and record.provenance == "contemporaneous":
        # Keep the staleness reminders in step until they read journal_entries directly.
        local_iso = datetime.fromisoformat(record.ts_utc.replace("Z", "+00:00"))
        try:
            from zoneinfo import ZoneInfo
            local_iso = local_iso.astimezone(ZoneInfo(record.tz))
        except Exception:
            pass
        with open(JOURNAL_LATEST_FILE, "w") as f:
            json.dump({"timestamp": local_iso.isoformat(),
                       "uploaded_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}, f, indent=2)

    logger.info("Journal quick entry %s (%s, %s)", "stored" if added else "duplicate",
                record.provenance, record.local_date)
    return {
        "status": "success",
        "id": record.id,
        "duplicate": not added,
        "local_date": record.local_date,
        "provenance": record.provenance,
        "enriched": sorted(json.loads(record.enrichment_json or "{}").keys()),
    }


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
    zip_path = JOURNAL_DIR / f"journal_{timestamp}.zip"
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