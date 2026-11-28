"""
database/journal/manage.py
~~~~~~~~~~~~~~~~~~~~~~~~~~
Queries behind the Dashboard's Journal page: day cards for days still missing an
entry, a day's detail (map track, places, spend, photos), and entry management.

Pure functions over an open sqlite3 connection (row_factory=sqlite3.Row).
"""

from datetime import date, datetime, timedelta, timezone
from typing import Optional

from database.journal.coverage import counted_days
from scheduled_tasks.daily_summary.base import get_date_context

CARD_COLUMNS = (
    "date", "timezone", "country", "city", "steps", "distance_km", "spend_gbp", "transaction_count",
    "photo_count", "temp_max_c", "temp_min_c", "weathercode", "sleep_hours", "was_in_transit",
    "distinct_places", "new_places_visited", "avg_valence", "mood_entries", "travel_phase", "day_label",
)
TRACK_MAX_POINTS = 400
LIST_LIMIT_MAX = 500
EDITABLE_FIELDS = ("text", "rating", "energy", "tags")


def _entry_brief(r) -> dict:
    return {"id": r["id"], "provenance": r["provenance"], "source": r["source"], "lag_days": r["lag_days"],
            "covers_start": r["covers_start"], "covers_end": r["covers_end"],
            "snippet": (r["text"] or "")[:160]}


_SPEND_FILTER = """
    t.is_internal = 0 AND t.is_interest = 0 AND (t.state != 'FAILED' OR t.state IS NULL)
    AND t.amount < 0 AND t.amount_gbp IS NOT NULL AND t.amount_gbp < 0
"""


def _spend_rows(rows) -> dict[str, list[dict]]:
    """Group (date, currency, local, gbp, n) rows into {date: [{currency, local, gbp, count}]}.

    Most-used currency first. Local amounts are in each transaction's own currency, so a day
    with two currencies yields two entries; gbp is the GBP equivalent of that currency's spend.
    """
    out: dict[str, list[dict]] = {}
    for r in rows:
        out.setdefault(r["date"], []).append({
            "currency": r["currency"], "local": round(r["local"], 2),
            "gbp": round(r["gbp"], 2), "count": r["n"]})
    for entries in out.values():
        entries.sort(key=lambda e: (-e["count"], e["currency"]))
    return out


def spend_by_day(conn, start: date, end: date) -> dict[str, list[dict]]:
    """Per-currency spend for each local day in [start, end], using each day's stored UTC window."""
    return _spend_rows(conn.execute(f"""
        SELECT ds.date AS date, t.currency AS currency, SUM(ABS(t.amount)) AS local,
               SUM(ABS(t.amount_gbp)) AS gbp, COUNT(*) AS n
        FROM daily_summary ds
        JOIN transactions t ON t.timestamp >= ds.utc_start AND t.timestamp < ds.utc_end
        WHERE ds.date >= ? AND ds.date <= ? AND {_SPEND_FILTER}
        GROUP BY ds.date, t.currency""", (start.isoformat(), end.isoformat())).fetchall())


def list_days(conn, mode: str, today: date, trip_start: date, retro_count_days: int) -> list[dict]:
    """Cards for each day from the trip start to yesterday, newest first.

    mode "gaps": only days with no entry that counts for reminders (same rule as
    database.journal.coverage). mode "all": every day, with the entries covering it.
    Entries too weak to count (an old single-day retro entry) still appear on the card.
    """
    end = today - timedelta(days=1)
    if end < trip_start:
        return []
    counted = counted_days(conn, trip_start, end, retro_count_days)

    cols = ", ".join(CARD_COLUMNS)
    summaries = {r["date"]: r for r in conn.execute(
        f"SELECT {cols} FROM daily_summary WHERE date >= ? AND date <= ?",
        (trip_start.isoformat(), end.isoformat()))}

    by_day: dict[str, list[dict]] = {}
    for r in conn.execute("""
        SELECT id, provenance, source, lag_days, covers_start, covers_end, local_date, text FROM journal_entries
        WHERE provenance IN ('contemporaneous', 'retro')
          AND COALESCE(covers_end, local_date) >= ? AND COALESCE(covers_start, local_date) <= ?
        ORDER BY ts_utc""", (trip_start.isoformat(), end.isoformat())):
        s = date.fromisoformat(r["covers_start"] or r["local_date"])
        e = date.fromisoformat(r["covers_end"] or r["local_date"])
        d = max(s, trip_start)
        while d <= min(e, end):
            by_day.setdefault(d.isoformat(), []).append(_entry_brief(r))
            d += timedelta(days=1)

    spend = spend_by_day(conn, trip_start, end)

    out = []
    d = end
    while d >= trip_start:
        iso = d.isoformat()
        if mode == "all" or d not in counted:
            card = {k: summaries[iso][k] for k in CARD_COLUMNS} if iso in summaries else {"date": iso}
            card["date"] = iso
            card["counted"] = d in counted
            card["spend"] = spend.get(iso, [])
            card["entries"] = by_day.get(iso, [])
            out.append(card)
        d -= timedelta(days=1)
    return out


def _downsample(rows: list, limit: int) -> list:
    if len(rows) <= limit:
        return rows
    step = len(rows) / limit
    return [rows[int(i * step)] for i in range(limit)] + [rows[-1]]


def day_detail(conn, day: str) -> dict:
    """Everything the enlarged card shows for one local day."""
    ctx = get_date_context(conn, day)
    lo, hi = ctx["utc_start"], ctx["utc_end"]

    cols = ", ".join(CARD_COLUMNS)
    row = conn.execute(f"SELECT {cols} FROM daily_summary WHERE date = ?", (day,)).fetchone()
    summary = {k: row[k] for k in CARD_COLUMNS} if row else {"date": day}
    if not summary.get("timezone"):
        summary["timezone"] = ctx["timezone"]

    track = [[r["longitude"], r["latitude"]] for r in _downsample(conn.execute(
        "SELECT longitude, latitude FROM location_unified WHERE timestamp >= ? AND timestamp < ? "
        "AND latitude IS NOT NULL ORDER BY timestamp", (lo, hi)).fetchall(), TRACK_MAX_POINTS)]

    visits = [dict(r) for r in conn.execute("""
        SELECT v.arrived_at, v.departed_at, v.duration_mins, k.label, k.latitude, k.longitude,
               p.city, p.locality
        FROM place_visits v JOIN known_places k ON k.id = v.known_place_id LEFT JOIN places p ON p.id = k.place_id
        WHERE v.arrived_at >= ? AND v.arrived_at < ? AND v.superseded_by IS NULL
        ORDER BY v.arrived_at""", (lo, hi))]

    transactions = [dict(r) for r in conn.execute(f"""
        SELECT t.timestamp, t.merchant, t.description, t.amount, t.currency, t.amount_gbp, t.category
        FROM transactions t
        WHERE t.timestamp >= ? AND t.timestamp < ? AND {_SPEND_FILTER}
        ORDER BY t.timestamp LIMIT 40""", (lo, hi))]
    spend = _spend_rows(conn.execute(f"""
        SELECT ? AS date, t.currency AS currency, SUM(ABS(t.amount)) AS local, SUM(ABS(t.amount_gbp)) AS gbp,
               COUNT(*) AS n FROM transactions t
        WHERE t.timestamp >= ? AND t.timestamp < ? AND {_SPEND_FILTER} GROUP BY t.currency""",
        (day, lo, hi)).fetchall()).get(day, [])

    photos = [dict(r) for r in conn.execute("""
        SELECT taken_at, latitude, longitude FROM photo_metadata
        WHERE taken_at >= ? AND taken_at < ? AND latitude IS NOT NULL
          AND is_screenshot = 0 AND is_screen_recording = 0
        ORDER BY taken_at LIMIT 60""", (lo, hi))]

    flights = [dict(r) for r in conn.execute("""
        SELECT origin_city, destination_city, origin_iata, destination_iata, departed_at, arrived_at
        FROM flights WHERE departed_at < ? AND arrived_at >= ? ORDER BY departed_at""", (hi, lo))]

    entries = [dict(r) for r in conn.execute("""
        SELECT id, provenance, source, lag_days, covers_start, covers_end, text, mood_self, rating, energy, tags
        FROM journal_entries
        WHERE provenance IN ('contemporaneous', 'retro')
          AND COALESCE(covers_start, local_date) <= ? AND COALESCE(covers_end, local_date) >= ?
        ORDER BY ts_utc""", (day, day))]

    return {"date": day, "utc_start": lo, "utc_end": hi, "summary": summary, "track": track,
            "visits": visits, "transactions": transactions, "spend": spend, "photos": photos, "flights": flights,
            "entries": entries}


# ---- entries ----------------------------------------------------------------

def list_entries(conn, q: Optional[str] = None, provenance: Optional[str] = None, source: Optional[str] = None,
                 date_from: Optional[str] = None, date_to: Optional[str] = None,
                 limit: int = 50, offset: int = 0) -> dict:
    where, args = [], []
    if q:
        where.append("text LIKE ? ESCAPE '\\'")
        args.append("%" + q.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%")
    if provenance:
        where.append("provenance = ?"); args.append(provenance)
    if source:
        where.append("source = ?"); args.append(source)
    if date_from:
        where.append("COALESCE(covers_end, local_date) >= ?"); args.append(date_from)
    if date_to:
        where.append("COALESCE(covers_start, local_date) <= ?"); args.append(date_to)
    clause = ("WHERE " + " AND ".join(where)) if where else ""
    limit = max(1, min(limit, LIST_LIMIT_MAX))

    total = conn.execute(f"SELECT COUNT(*) FROM journal_entries {clause}", args).fetchone()[0]
    rows = conn.execute(f"""
        SELECT id, ts_utc, local_date, covers_start, covers_end, source, provenance, lag_days, text,
               rating, energy, tags, mood_self, valence_self, place_id, created_at
        FROM journal_entries {clause} ORDER BY COALESCE(covers_end, local_date) DESC, ts_utc DESC
        LIMIT ? OFFSET ?""", [*args, limit, max(0, offset)]).fetchall()
    return {"total": total, "entries": [dict(r) for r in rows]}


def get_entry(conn, entry_id: str):
    return conn.execute("SELECT * FROM journal_entries WHERE id = ?", (entry_id,)).fetchone()


def update_entry(conn, entry_id: str, fields: dict) -> bool:
    """Update editable fields. Raises ValueError for invalid values. Returns False if not found."""
    sets, args = [], []
    for k in EDITABLE_FIELDS:
        if k not in fields:
            continue
        v = fields[k]
        if k == "text":
            if not isinstance(v, str) or not v.strip():
                raise ValueError("text cannot be empty")
            v = v.strip()
        elif k in ("rating", "energy"):
            lim = 10 if k == "rating" else 5
            if v in ("", None, 0):
                v = None
            elif not isinstance(v, int) or not 1 <= v <= lim:
                raise ValueError(f"{k} must be 1-{lim}")
        elif k == "tags":
            v = v.strip() or None if isinstance(v, str) else None
        sets.append(f"{k} = ?"); args.append(v)
    if not sets:
        raise ValueError("nothing to update")
    cur = conn.execute(f"UPDATE journal_entries SET {', '.join(sets)} WHERE id = ?", [*args, entry_id])
    return cur.rowcount > 0


def delete_entry(conn, entry_id: str) -> bool:
    return conn.execute("DELETE FROM journal_entries WHERE id = ?", (entry_id,)).rowcount > 0


def export_entries(conn, fmt: str) -> tuple[str, str, str]:
    """Return (content, media_type, filename) for all entries, oldest first."""
    rows = [dict(r) for r in conn.execute("SELECT * FROM journal_entries ORDER BY COALESCE(covers_start, local_date), ts_utc")]
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    if fmt == "md":
        parts = ["# Journal\n"]
        for r in rows:
            span = r["covers_start"] if r["covers_start"] == r["covers_end"] else f"{r['covers_start']} to {r['covers_end']}"
            note = f"{r['provenance']}" + (f", written {r['lag_days']} days later" if r["lag_days"] else "")
            parts.append(f"## {span or r['local_date']}\n\n*{note}*\n\n{r['text']}\n")
        return "\n".join(parts), "text/markdown", f"journal_{stamp}.md"
    import json
    return json.dumps(rows, indent=2, default=str), "application/json", f"journal_{stamp}.json"
