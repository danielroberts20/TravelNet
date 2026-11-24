"""
health_gaps/events.py
~~~~~~~~~~~~~~~~~~~~~
Persisted gap-event state machine.

reconcile() takes the candidate events found by today's scan and brings the
health_gap_events table up to date. It returns what is newly worth telling
the user about; it does not send anything itself.

Manual export workflow
----------------------
A confirmed gap is something only a manual Health Auto Export run can fix.
After doing the export the user checks the event off (mark_exported). The next
daily run re-scans the window: if the data is now present the event becomes
`resolved`; if it is still empty the event becomes `unrecoverable` (genuinely
not there) and is never reported again.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

from health_gaps.config import CRITICAL_METRIC_COUNT, config_for
from health_gaps.detect import Candidate, fmt, parse

OPEN, CONFIRMED, RESOLVED, UNRECOVERABLE = "open", "confirmed", "resolved", "unrecoverable"
LIVE = (OPEN, CONFIRMED)


@dataclass(frozen=True)
class Params:
    min_points_after: int = 5
    escalate_days: float = 3.0
    export_verify_delay_h: float = 1.0
    export_pad_h: float = 2.0


@dataclass
class Event:
    id: int
    group_key: str
    metrics: dict[str, list]
    window_start: datetime
    window_end: Optional[datetime]
    status: str
    critical: bool
    immediate: bool
    notified_status: Optional[str]
    pushed_at: Optional[str]
    exported_at: Optional[str]

    @property
    def hours(self) -> Optional[float]:
        if self.window_end is None:
            return None
        return (self.window_end - self.window_start).total_seconds() / 3600

    def export_range(self, now: datetime, pad_h: float) -> tuple[datetime, datetime]:
        pad = timedelta(hours=pad_h)
        return self.window_start - pad, (self.window_end or now) + pad


@dataclass
class Report:
    """What the daily run found worth telling the user."""
    newly_confirmed: list[Event] = field(default_factory=list)   # to notify now
    held_for_digest: list[Event] = field(default_factory=list)   # confirmed, digest-only, not yet told
    awaiting_export: list[Event] = field(default_factory=list)   # confirmed, no export checked off
    ongoing: list[Event] = field(default_factory=list)           # open
    resolved: list[Event] = field(default_factory=list)
    unrecoverable: list[Event] = field(default_factory=list)
    to_push: list[Event] = field(default_factory=list)           # critical, not yet pushed


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _row_to_event(row: sqlite3.Row) -> Event:
    return Event(
        id=row["id"],
        group_key=row["group_key"],
        metrics=json.loads(row["metrics"]),
        window_start=parse(row["window_start"]),
        window_end=parse(row["window_end"]) if row["window_end"] else None,
        status=row["status"],
        critical=bool(row["critical"]),
        immediate=bool(row["immediate"]),
        notified_status=row["notified_status"],
        pushed_at=row["pushed_at"],
        exported_at=row["exported_at"],
    )


def get_event(conn: sqlite3.Connection, event_id: int) -> Optional[Event]:
    row = conn.execute("SELECT * FROM health_gap_events WHERE id = ?", (event_id,)).fetchone()
    return _row_to_event(row) if row else None


def list_events(conn: sqlite3.Connection, statuses: tuple[str, ...] | None = None) -> list[Event]:
    sql = "SELECT * FROM health_gap_events"
    args: tuple = ()
    if statuses:
        sql += f" WHERE status IN ({','.join('?' * len(statuses))})"
        args = statuses
    return [_row_to_event(r) for r in conn.execute(sql + " ORDER BY window_start", args)]


def earliest_live_start(conn: sqlite3.Connection) -> Optional[datetime]:
    """Start of the oldest open/confirmed event, so the scan window can reach it."""
    row = conn.execute(
        "SELECT MIN(window_start) FROM health_gap_events WHERE status IN ('open','confirmed')"
    ).fetchone()
    return parse(row[0]) if row and row[0] else None


def _flags(metrics: dict) -> tuple[bool, bool]:
    cfgs = [config_for(m) for m in metrics]
    cfgs = [c for c in cfgs if c]
    # Digest-only metrics (walking quartet, ...) never make an outage "critical".
    n_loud = sum(1 for c in cfgs if c.immediate)
    critical = n_loud >= CRITICAL_METRIC_COUNT or any(c.critical for c in cfgs)
    immediate = any(c.immediate for c in cfgs) or critical
    return critical, immediate


def _decide_status(cand: Candidate, current: str, now: datetime, p: Params) -> str:
    """Status implied by the evidence. A confirmed event is never downgraded."""
    if current == CONFIRMED:
        return CONFIRMED
    escalate = timedelta(days=p.escalate_days)
    if cand.end is None:                       # ongoing
        return CONFIRMED if now - cand.start >= escalate else OPEN
    if cand.points_after >= p.min_points_after:  # closed and the feed is back
        return CONFIRMED
    return CONFIRMED if now - cand.end >= escalate else OPEN


def _overlaps(ev: Event, cand: Candidate, now: datetime) -> bool:
    ev_end = ev.window_end or now
    c_end = cand.end or now
    return ev.group_key == cand.group_key and cand.start <= ev_end and ev.window_start <= c_end


def _metrics_json(cand: Candidate) -> str:
    return json.dumps({m: [fmt(s), fmt(e) if e else None] for m, (s, e) in cand.metrics.items()})


# ---------------------------------------------------------------------------
# reconcile
# ---------------------------------------------------------------------------

def reconcile(
    conn: sqlite3.Connection,
    candidates: list[Candidate],
    now: datetime,
    scan_start: datetime,
    params: Params = Params(),
) -> Report:
    now_s = fmt(now)
    events = list_events(conn)
    matched: set[int] = set()
    verify_after = timedelta(hours=params.export_verify_delay_h)
    report = Report()

    for cand in candidates:
        ev = next((e for e in events if _overlaps(e, cand, now)), None)
        critical, immediate = _flags(cand.metrics)

        if ev is None:
            status = _decide_status(cand, OPEN, now, params)
            conn.execute(
                """
                INSERT INTO health_gap_events
                    (group_key, metrics, window_start, window_end, status, critical,
                     immediate, first_seen, last_seen)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (cand.group_key, _metrics_json(cand), fmt(cand.start),
                 fmt(cand.end) if cand.end else None, status, int(critical),
                 int(immediate), now_s, now_s),
            )
            continue

        matched.add(ev.id)
        if ev.status == UNRECOVERABLE:
            continue                                    # known permanent loss

        status = _decide_status(cand, OPEN if ev.status == RESOLVED else ev.status, now, params)
        resolved_at = None
        if ev.exported_at and now - parse(ev.exported_at) >= verify_after:
            status = UNRECOVERABLE                      # exported, still empty
            resolved_at = now_s

        merged = dict(ev.metrics)
        merged.update(json.loads(_metrics_json(cand)))
        if status == UNRECOVERABLE:
            merged = json.loads(_metrics_json(cand))    # only what is still missing
        start = min(ev.window_start, cand.start)
        end = cand.end
        c2, i2 = _flags(merged)
        conn.execute(
            """
            UPDATE health_gap_events
               SET metrics = ?, window_start = ?, window_end = ?, status = ?,
                   critical = ?, immediate = ?, last_seen = ?, resolved_at = COALESCE(?, resolved_at)
             WHERE id = ?
            """,
            (json.dumps(merged), fmt(start), fmt(end) if end else None, status,
             int(c2), int(i2), now_s, resolved_at, ev.id),
        )

    # Live events no longer observed inside the scan window: the data arrived.
    for ev in events:
        if ev.id in matched or ev.status not in LIVE:
            continue
        if ev.window_start >= scan_start:
            conn.execute(
                "UPDATE health_gap_events SET status = ?, resolved_at = ?, last_seen = ? WHERE id = ?",
                (RESOLVED, now_s, now_s, ev.id),
            )

    conn.commit()
    return _build_report(conn, report)


def _build_report(conn: sqlite3.Connection, report: Report) -> Report:
    for ev in list_events(conn):
        if ev.status == OPEN:
            report.ongoing.append(ev)
        elif ev.status == CONFIRMED:
            if ev.notified_status != CONFIRMED:
                (report.newly_confirmed if ev.immediate else report.held_for_digest).append(ev)
            if not ev.exported_at:
                report.awaiting_export.append(ev)
            if ev.critical and not ev.pushed_at:
                report.to_push.append(ev)
        elif ev.status == RESOLVED and ev.notified_status == CONFIRMED:
            report.resolved.append(ev)
    return report


# ---------------------------------------------------------------------------
# notification bookkeeping + user actions
# ---------------------------------------------------------------------------

def mark_notified(conn: sqlite3.Connection, event_ids: list[int], now: datetime) -> None:
    now_s = fmt(now)
    for i in event_ids:
        conn.execute(
            "UPDATE health_gap_events SET notified_status = status, last_notified = ? WHERE id = ?",
            (now_s, i),
        )
    conn.commit()


def mark_pushed(conn: sqlite3.Connection, event_ids: list[int], now: datetime) -> None:
    for i in event_ids:
        conn.execute("UPDATE health_gap_events SET pushed_at = ? WHERE id = ?", (fmt(now), i))
    conn.commit()


class EventStateError(Exception):
    pass


def mark_exported(conn: sqlite3.Connection, event_id: int, now: datetime) -> Event:
    """User ran a manual HAE export for this gap. Verified on the next daily run."""
    ev = get_event(conn, event_id)
    if ev is None:
        raise LookupError(event_id)
    if ev.status != CONFIRMED:
        raise EventStateError(f"event {event_id} is {ev.status}; only confirmed gaps can be checked off")
    conn.execute("UPDATE health_gap_events SET exported_at = ? WHERE id = ?", (fmt(now), event_id))
    conn.commit()
    return get_event(conn, event_id)  # type: ignore[return-value]


def dismiss(conn: sqlite3.Connection, event_id: int, now: datetime, note: Optional[str] = None) -> Event:
    """Declare the gap genuinely absent without exporting (e.g. watch was off)."""
    ev = get_event(conn, event_id)
    if ev is None:
        raise LookupError(event_id)
    if ev.status not in LIVE:
        raise EventStateError(f"event {event_id} is already {ev.status}")
    conn.execute(
        "UPDATE health_gap_events SET status = ?, resolved_at = ?, note = ? WHERE id = ?",
        (UNRECOVERABLE, fmt(now), note, event_id),
    )
    conn.commit()
    return get_event(conn, event_id)  # type: ignore[return-value]
