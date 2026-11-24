"""
health_gaps/backfill.py
~~~~~~~~~~~~~~~~~~~~~~~
Scan the whole health history for gaps in one pass, silently.

The daily check only looks back a week, so older gaps were never recorded. A backfill scan
runs the same detector over the entire history and reconciles the results into
health_gap_events, but:

  * it sends no per-event email or push (the events are marked as told, and the user gets one
    summary plus a CSV to review instead), so ~100 events cannot flood anyone;
  * it is dry by default: the events are reconciled into a throwaway in-memory copy of the
    current events table, so nothing changes until ``persist=True``.

Recovery of what the scan finds is the normal recovery flow, run in budgeted sessions
(health_gaps/recover.py: tiers, max_requests).

Free of Prefect and the settings object.
"""

from __future__ import annotations

import csv
import io
import sqlite3
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime

from database.health.gap_events.table import init_schema
from health_gaps import events as ev_mod
from health_gaps.detect import cluster_gaps
from health_gaps.events import Event, Params
from health_gaps.pull import estimate_requests
from health_gaps.recover import recovery_tier
from health_gaps.runner import HealthData, find_gaps

TIER_LABEL = {1: "loud (watch vitals, activity totals, sleep)", 2: "walking (digest-only)", 3: "sparse-only (VO2 Max, ...)"}
REQUEST_SECONDS = 0.8     # measured against the phone: 88 requests in ~70 s


@dataclass
class BackfillResult:
    scan_from: datetime
    now: datetime
    persisted: bool
    events: list[Event] = field(default_factory=list)
    new_event_ids: set[int] = field(default_factory=set)       # not in the table before this scan
    requests: dict[int, int] = field(default_factory=dict)     # event id -> estimated HAE requests

    def tier_stats(self) -> dict[int, tuple[int, int]]:
        """tier -> (events, requests) over the confirmed events a recovery would pull."""
        stats: dict[int, list[int]] = {1: [0, 0], 2: [0, 0], 3: [0, 0]}
        for e in self.events:
            if e.status == ev_mod.CONFIRMED:
                t = recovery_tier(e)
                stats[t][0] += 1
                stats[t][1] += self.requests.get(e.id, 0)
        return {t: (n, r) for t, (n, r) in stats.items()}

    @property
    def total_requests(self) -> int:
        return sum(r for _, r in self.tier_stats().values())


def copy_events(src: sqlite3.Connection) -> sqlite3.Connection:
    """In-memory copy of health_gap_events, so a dry scan can reconcile without touching the real table."""
    mem = sqlite3.connect(":memory:")
    mem.row_factory = sqlite3.Row
    init_schema(mem)
    cols = [r[1] for r in mem.execute("PRAGMA table_info(health_gap_events)")]
    src.row_factory = sqlite3.Row
    for row in src.execute("SELECT * FROM health_gap_events"):
        keep = [c for c in cols if c in row.keys()]
        mem.execute(
            f"INSERT INTO health_gap_events ({','.join(keep)}) VALUES ({','.join('?' * len(keep))})",
            [row[c] for c in keep],
        )
    mem.commit()
    return mem


def run_backfill_scan(
    state_conn: sqlite3.Connection,
    data: HealthData,
    now: datetime,
    scan_from: datetime,
    *,
    min_nights: int = 2,
    params: Params = Params(),
    pad_h: float = 12.0,
    persisted: bool = False,
) -> BackfillResult:
    """Detect over ``data`` (loaded from ``scan_from``) and reconcile into ``state_conn``.

    Whoever calls this decides whether ``state_conn`` is the real table or a copy. Everything
    newly confirmed is marked as notified/pushed: the summary replaces per-event messages.
    """
    before = {e.id for e in ev_mod.list_events(state_conn)}
    candidates = cluster_gaps(find_gaps(data, now, min_nights), now)
    report = ev_mod.reconcile(state_conn, candidates, now, scan_from, params)
    told = report.newly_confirmed + report.held_for_digest
    ev_mod.mark_notified(state_conn, [e.id for e in told], now)
    ev_mod.mark_pushed(state_conn, [e.id for e in report.to_push], now)

    events = ev_mod.list_events(state_conn)
    return BackfillResult(
        scan_from=scan_from, now=now, persisted=persisted, events=events,
        new_event_ids={e.id for e in events} - before,
        requests={e.id: estimate_requests(e, now, pad_h) for e in events if e.status == ev_mod.CONFIRMED},
    )


# ---------------------------------------------------------------------------
# reporting
# ---------------------------------------------------------------------------

def build_summary(r: BackfillResult, max_requests: int) -> tuple[str, str]:
    by_status = Counter(e.status for e in r.events)
    stats = r.tier_stats()
    total = r.total_requests
    sessions = -(-total // max_requests) if max_requests and total else 1 if total else 0
    mode = "WRITTEN to the database" if r.persisted else "DRY RUN (nothing was saved)"
    subject = f"[TravelNet] Health history scan — {len(r.new_event_ids)} new gaps ({'written' if r.persisted else 'dry run'})"
    lines = [
        f"Health history scan — {r.scan_from:%Y-%m-%d} to {r.now:%Y-%m-%d}   [{mode}]",
        "",
        f"Events now: {len(r.events)}  (" + ", ".join(f"{n} {s}" for s, n in sorted(by_status.items())) + ")",
        f"New in this scan: {len(r.new_event_ids)}",
        "",
        "Recovery workload (confirmed gaps a pull could still help):",
    ]
    for t in (1, 2, 3):
        n, req = stats[t]
        lines.append(f"  tier {t}  {TIER_LABEL[t]:<45} {n:>3} events  {req:>5} requests  (~{req * REQUEST_SECONDS / 60:4.1f} min)")
    lines += [
        f"  total{'':<56}{total:>5} requests  (~{total * REQUEST_SECONDS / 60:4.1f} min)",
        "",
        f"At {max_requests} requests per session that is about {sessions} session(s), loud gaps first, newest first.",
        "Each session needs the HAE server running with the phone unlocked; you will be notified when to start it.",
    ]
    return subject, "\n".join(lines)


def events_csv(r: BackfillResult) -> str:
    """One row per event, for review: id, tier, status, group, metrics, window, hours, est. requests."""
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["id", "tier", "status", "group", "metrics", "start_utc", "end_utc", "hours", "est_requests", "new"])
    for e in sorted(r.events, key=lambda e: e.window_start):
        w.writerow([
            e.id, recovery_tier(e), e.status, e.group_key, "; ".join(sorted(e.metrics)),
            e.window_start.strftime("%Y-%m-%d %H:%M"),
            e.window_end.strftime("%Y-%m-%d %H:%M") if e.window_end else "ongoing",
            f"{e.hours:.1f}" if e.hours is not None else "",
            r.requests.get(e.id, ""), int(e.id in r.new_event_ids),
        ])
    return out.getvalue()
