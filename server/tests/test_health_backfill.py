"""Tests for health_gaps/backfill.py: silent whole-history scan, dry by default."""

import csv
import io
import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from database.health.gap_events.table import init_schema
from health_gaps import events as ev
from health_gaps.backfill import build_summary, copy_events, events_csv, run_backfill_scan
from health_gaps.runner import HealthData, compute_scan_start, run_check

UTC = timezone.utc


def T(s):
    return datetime.strptime(s, "%Y-%m-%d %H:%M").replace(tzinfo=UTC)


def hourly(start, end, holes=()):
    out, t = [], T(start)
    while t <= T(end):
        if not any(T(a) <= t < T(b) for a, b in holes):
            out.append(t)
        t += timedelta(hours=1)
    return out


NOW = T("2026-11-24 09:00")
FIRST = T("2026-06-10 00:00")
# three loud (Step Count) gaps spread across the history, one walking (digest-only) gap
HOLES = [("2026-07-01 00:00", "2026-07-03 00:00"), ("2026-08-10 00:00", "2026-08-12 12:00"), ("2026-10-02 00:00", "2026-10-04 00:00")]
WALK_HOLES = [("2026-09-01 00:00", "2026-09-03 00:00")]


def history():
    series = {"Step Count": hourly("2026-06-10 00:00", "2026-11-23 23:00", HOLES),
              "Walking Speed": hourly("2026-06-10 00:00", "2026-11-23 23:00", WALK_HOLES)}
    return HealthData(series=series, last_seen={m: ts[-1] for m, ts in series.items()})


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    init_schema(c)
    return c


def test_whole_history_is_scanned_in_one_pass(conn):
    r = run_backfill_scan(conn, history(), NOW, FIRST)
    starts = sorted(e.window_start.strftime("%m-%d") for e in r.events)
    # a gap starts at the last point before the hole
    assert starts == ["06-30", "08-09", "08-31", "10-01"]
    assert len(r.events) == 4 and r.new_event_ids == {e.id for e in r.events}
    assert all(e.status == "confirmed" for e in r.events)


def test_requests_estimated_and_tiered(conn):
    r = run_backfill_scan(conn, history(), NOW, FIRST)
    stats = r.tier_stats()
    assert stats[1][0] == 3 and stats[2][0] == 1 and stats[3] == (0, 0)
    assert stats[1][1] > 0 and stats[2][1] > 0 and r.total_requests == stats[1][1] + stats[2][1]


def test_scan_is_silent_and_marks_events_as_told(conn):
    run_backfill_scan(conn, history(), NOW, FIRST)
    assert all(e.notified_status == "confirmed" for e in ev.list_events(conn))
    # the next normal daily check must not email about any of them
    emails, pushes = [], []
    data = history()
    scan_start = compute_scan_start(conn, NOW + timedelta(days=1), 7)
    run_check(conn, data, NOW + timedelta(days=1), scan_start, send_email=lambda s, b: emails.append(s), push=pushes.append)
    assert emails == [] and pushes == []


def test_rescanning_is_idempotent(conn):
    run_backfill_scan(conn, history(), NOW, FIRST)
    again = run_backfill_scan(conn, history(), NOW, FIRST)
    assert len(again.events) == 4 and again.new_event_ids == set()


def test_unrecoverable_events_stay_unrecoverable(conn):
    first = run_backfill_scan(conn, history(), NOW, FIRST)
    ev.dismiss(conn, first.events[0].id, NOW, "watch off")
    again = run_backfill_scan(conn, history(), NOW, FIRST)
    assert next(e for e in again.events if e.id == first.events[0].id).status == "unrecoverable"
    assert len(again.events) == 4


def test_dry_run_copy_leaves_the_real_table_untouched(conn):
    conn.execute("INSERT INTO health_gap_events (group_key, metrics, window_start, window_end, status, first_seen, last_seen)"
                 " VALUES ('g', '{}', '2026-06-01T00:00:00Z', '2026-06-02T00:00:00Z', 'resolved', 'x', 'x')")
    conn.commit()
    mem = copy_events(conn)
    assert mem.execute("SELECT COUNT(*) FROM health_gap_events").fetchone()[0] == 1
    run_backfill_scan(mem, history(), NOW, FIRST)
    assert mem.execute("SELECT COUNT(*) FROM health_gap_events").fetchone()[0] == 5
    assert conn.execute("SELECT COUNT(*) FROM health_gap_events").fetchone()[0] == 1       # real one unchanged


def test_summary_states_mode_and_session_count(conn):
    r = run_backfill_scan(conn, history(), NOW, FIRST)
    subject, body = build_summary(r, max_requests=100)
    assert "DRY RUN" in body and "dry run" in subject and "Events now: 4" in body
    sessions = -(-r.total_requests // 100)
    assert f"about {sessions} session(s)" in body and "tier 1" in body and "tier 2" in body
    r.persisted = True
    assert "WRITTEN" in build_summary(r, 100)[1]
    assert "about 1 session(s)" in build_summary(r, 0)[1]          # 0 = unlimited


def test_csv_lists_every_event_with_tier_and_estimate(conn):
    r = run_backfill_scan(conn, history(), NOW, FIRST)
    rows = list(csv.DictReader(io.StringIO(events_csv(r))))
    assert len(rows) == 4 and [row["start_utc"][:10] for row in rows] == sorted(row["start_utc"][:10] for row in rows)
    assert {row["tier"] for row in rows} == {"1", "2"}
    assert all(int(row["est_requests"]) > 0 and row["new"] == "1" for row in rows)
