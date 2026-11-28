"""
Journal management API used by the Dashboard's Journal page (upload-token auth,
mounted under /upload/journal alongside the capture endpoints).

  GET    /days?mode=gaps|all     day cards (default: days still missing an entry)
  GET    /days/{day}             one day's detail: map track, places, spend, photos, entries
  GET    /entries                list / search / filter
  PATCH  /entries/{id}           edit text, rating, energy, tags
  DELETE /entries/{id}
  POST   /sync                   push every entry to Trevor (full reconcile)
  GET    /export?format=json|md
"""

import logging
from datetime import date, datetime, timezone
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, Response  # type: ignore
from pydantic import BaseModel, Field  # type: ignore

import config.general as cfg
from auth import require_upload_token
from database.connection import get_conn
from database.journal import manage
from database.journal.enrich import latest_timezone
from database.journal.sync import push_entry_ids, reconcile_all

router = APIRouter(dependencies=[Depends(require_upload_token)])
logger = logging.getLogger(__name__)


def _parse_day(day: str) -> date:
    try:
        return date.fromisoformat(day)
    except ValueError:
        raise HTTPException(status_code=400, detail="day must be YYYY-MM-DD")


@router.get("/days")
def get_days(mode: str = Query("gaps", pattern="^(gaps|all)$")):
    with get_conn(read_only=True) as conn:
        today = datetime.now(timezone.utc).astimezone(latest_timezone(conn)).date()
        days = manage.list_days(conn, mode, today, cfg.TRAVEL_START_DATE.date(), cfg.JOURNAL_RETRO_COUNT_DAYS)
    return {"mode": mode, "count": len(days), "days": days}


@router.get("/days/{day}")
def get_day(day: str):
    _parse_day(day)
    with get_conn(read_only=True) as conn:
        return manage.day_detail(conn, day)


@router.get("/entries")
def get_entries(q: Optional[str] = None, provenance: Optional[str] = None, source: Optional[str] = None,
                date_from: Optional[str] = None, date_to: Optional[str] = None,
                limit: int = 50, offset: int = 0):
    for d in (date_from, date_to):
        if d:
            _parse_day(d)
    with get_conn(read_only=True) as conn:
        return manage.list_entries(conn, q, provenance, source, date_from, date_to, limit, offset)


class EntryUpdate(BaseModel):
    text: Optional[str] = Field(default=None, max_length=50000)
    rating: Optional[int] = None
    energy: Optional[int] = None
    tags: Optional[str] = None


@router.patch("/entries/{entry_id}")
def patch_entry(entry_id: str, body: EntryUpdate, background_tasks: BackgroundTasks):
    fields = body.model_dump(exclude_unset=True)
    with get_conn() as conn:
        try:
            found = manage.update_entry(conn, entry_id, fields)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
    if not found:
        raise HTTPException(status_code=404, detail="Entry not found")
    background_tasks.add_task(push_entry_ids, [entry_id])     # Trevor re-embeds if the text changed
    return {"status": "success", "id": entry_id}


def _after_delete(days: list[str]) -> None:
    from scheduled_tasks.daily_summary.journal import refresh_journal_days
    try:
        refresh_journal_days(days)
    except Exception as e:
        logger.error("daily_summary journal refresh failed after delete: %s", e)
    reconcile_all()      # drops the deleted document from Trevor


@router.delete("/entries/{entry_id}")
def remove_entry(entry_id: str, background_tasks: BackgroundTasks):
    from datetime import timedelta
    with get_conn() as conn:
        row = manage.get_entry(conn, entry_id)
        if row is None:
            raise HTTPException(status_code=404, detail="Entry not found")
        s = date.fromisoformat(row["covers_start"] or row["local_date"])
        e = date.fromisoformat(row["covers_end"] or row["local_date"])
        days = [(s + timedelta(days=i)).isoformat() for i in range((e - s).days + 1)]
        manage.delete_entry(conn, entry_id)
    background_tasks.add_task(_after_delete, days)
    return {"status": "success", "id": entry_id}


@router.post("/sync")
def sync_all(background_tasks: BackgroundTasks):
    with get_conn(read_only=True) as conn:
        n = conn.execute("SELECT COUNT(*) FROM journal_entries").fetchone()[0]
    background_tasks.add_task(reconcile_all)
    return {"status": "accepted", "entries": n}


@router.get("/export")
def export(format: str = Query("json", pattern="^(json|md)$")):
    with get_conn(read_only=True) as conn:
        content, media_type, filename = manage.export_entries(conn, format)
    return Response(content=content, media_type=media_type,
                    headers={"Content-Disposition": f'attachment; filename="{filename}"'})
