"""
database/journal/sync.py
~~~~~~~~~~~~~~~~~~~~~~~~
Push journal_entries to Trevor.

TravelNet is the source of truth; Trevor's Chroma store is a derived index. We
push (rather than have Trevor read travel.db) so Trevor needs no runtime access
to this database and sees exactly what TravelNet resolved: final valence,
labels, place. A full reconcile (`reconcile=True`) also tells Trevor to drop
documents for rows that no longer exist.

Never raises: sync failures are logged and retried by the nightly reconcile.
"""

import json
import logging
from typing import Iterable, Optional

import httpx

from config.settings import settings
from database.connection import get_conn
from database.journal.mood import resolve_mood

logger = logging.getLogger(__name__)


def build_trevor_entry(conn, row) -> dict:
    """Flatten a journal_entries row (plus resolved mood and place) for Trevor."""
    extra = json.loads(row["enrichment_json"] or "{}")
    loc = extra.get("location") or {}
    mood = resolve_mood(conn, row)
    return {
        "id": row["id"],
        "text": row["text"],
        "ts_utc": row["ts_utc"],
        "local_date": row["local_date"],
        "utc_offset": row["utc_offset"],
        "tz": row["tz"],
        "covers_start": row["covers_start"] or row["local_date"],
        "covers_end": row["covers_end"] or row["local_date"],
        "lag_days": row["lag_days"],
        "source": row["source"],
        "provenance": row["provenance"],
        "town": loc.get("town") or loc.get("city"),
        "country": loc.get("country"),
        "latitude": row["latitude"],
        "longitude": row["longitude"],
        "energy": row["energy"],
        "rating": row["rating"],
        "tags": row["tags"],
        "valence": mood["valence"],
        "valence_source": mood["valence_source"],
        "valence_class": mood["valence_class"],
        "mood_labels": mood["labels"],
        "mood_associations": mood["associations"],
    }


def push_to_trevor(entries: list[dict], reconcile: bool = False) -> bool:
    """POST entries to Trevor. With reconcile=True the list is the complete set."""
    if reconcile and not entries:
        logger.warning("Journal reconcile skipped: refusing to send an empty full set")
        return False
    try:
        resp = httpx.post(
            f"{settings.trevor_url}/ingest/entries",
            json={"entries": entries, "reconcile": reconcile},
            headers={"x-api-key": settings.trevor_api_key},
            timeout=120,
        )
        if resp.status_code == 200:
            logger.info("Pushed %d journal entr%s to Trevor%s", len(entries),
                        "y" if len(entries) == 1 else "ies", " (reconcile)" if reconcile else "")
            return True
        logger.warning("Trevor journal sync returned %d: %s", resp.status_code, resp.text[:200])
    except Exception as e:
        logger.error("Trevor journal sync failed: %s", e)
    return False


def push_entry_ids(ids: Iterable[str]) -> bool:
    """Build and push the given entries (background-task friendly: own connection)."""
    ids = list(ids)
    if not ids:
        return True
    with get_conn(read_only=True) as conn:
        marks = ",".join("?" for _ in ids)
        rows = conn.execute(f"SELECT * FROM journal_entries WHERE id IN ({marks})", ids).fetchall()
        entries = [build_trevor_entry(conn, r) for r in rows]
    return push_to_trevor(entries)


def reconcile_all() -> Optional[int]:
    """Send every entry as the full set. Returns the number sent, or None on failure."""
    with get_conn(read_only=True) as conn:
        rows = conn.execute("SELECT * FROM journal_entries ORDER BY ts_utc").fetchall()
        entries = [build_trevor_entry(conn, r) for r in rows]
    return len(entries) if push_to_trevor(entries, reconcile=True) else None
