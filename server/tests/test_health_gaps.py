"""Tests for persistent health gap detection (health_gaps/*).

Regression fixtures come from real production gaps:
  * 22-23 Oct 2026  walking quartet gap (07:55 -> 11:10, 27.2h)  - reported exactly once
  * 24-25 Oct 2026  Watch vitals outage across 5 metrics          - one grouped event
"""

import sqlite3
from datetime import date, datetime, timedelta, timezone

import pytest

from database.health.gap_events.table import init_schema
from health_gaps import events as ev
from health_gaps.config import config_for
from health_gaps.detect import cluster_gaps, detect_metric_gaps, detect_sleep_gaps
from health_gaps.runner import HealthData, compute_scan_start, run_check


def T(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)


WALKING = ["Walking Speed", "Walking Step Length",
           "Walking Double Support Percentage", "Walking Asymmetry Percentage"]
# (metric, last point before, first point after) - real production values, 24-25 Oct
OUTAGE = {
    "heart_rate":                   ("2026-10-24 22:05", "2026-10-25 22:20"),
    "Blood Oxygen Saturation":      ("2026-10-24 20:40", "2026-10-25 23:25"),
    "Environmental Audio Exposure": ("2026-10-24 20:45", "2026-10-25 22:50"),
    "Heart Rate Variability":       ("2026-10-24 20:45", "2026-10-25 23:20"),
    "Time in Daylight":             ("2026-10-24 21:15", "2026-10-26 00:10"),
}


def series(start: str, end: str, step_h: float = 1.0, hole: tuple[str, str] | None = None):
    pts, t, stop = [], T(start), T(end)
    while t <= stop:
        pts.append(t)
        t += timedelta(hours=step_h)
    if hole:
        a, b = T(hole[0]), T(hole[1])
        pts = [p for p in pts if p <= a or p >= b]
        pts += [a, b]
    return sorted(set(pts))


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    init_schema(c)
    return c


class Harness:
    """Runs the daily check against full synthetic series as of a given 'now'."""

    def __init__(self, conn, full: dict, sleep_nights=None):
        self.conn, self.full, self.sleep_nights = conn, full, sleep_nights or {}
        self.emails, self.pushes = [], []

    def run(self, now_s: str, **kw):
        now = T(now_s)
        scan_start = compute_scan_start(self.conn, now, 7)
        data = HealthData(
            series={m: [t for t in ts if scan_start <= t <= now] for m, ts in self.full.items()},
            last_seen={m: max(t for t in ts if t <= now) for m, ts in self.full.items()
                       if any(t <= now for t in ts)},
            sleep_nights={d: n for d, n in self.sleep_nights.items() if d < now.date()},
        )
        return run_check(
            self.conn, data, now, scan_start,
            send_email=lambda s, b: self.emails.append((s, b)),
            push=self.pushes.append, **kw,
        )

    def statuses(self):
        return [(e.id, e.status) for e in ev.list_events(self.conn)]


def healthy_continuous(extra_start="2026-10-10 00:00", end="2026-11-10 00:00"):
    return {"Step Count": series(extra_start, end, 1.0)}


# --------------------------------------------------------------------------
# detection / config
# --------------------------------------------------------------------------

def test_sleep_only_metric_overnight_gap_is_not_flagged():
    """Respiratory Rate normally goes 40h+ between samples; absolute 72h threshold."""
    ts = series("2026-10-20 00:00", "2026-10-30 00:00", 0.2, hole=("2026-10-20 23:15", "2026-10-22 16:30"))
    assert detect_metric_gaps("Respiratory Rate", ts, T("2026-10-30 01:00")) == []


def test_continuous_metric_flags_gap_over_threshold():
    ts = series("2026-10-24 00:00", "2026-10-26 12:00", 0.5, hole=("2026-10-24 20:40", "2026-10-25 23:25"))
    gaps = detect_metric_gaps("Blood Oxygen Saturation", ts, T("2026-10-26 13:00"))
    assert len(gaps) == 1 and gaps[0].end == T("2026-10-25 23:25")


def test_unmonitored_sparse_metric_is_ignored():
    ts = [T("2026-10-01 00:00")]
    assert detect_metric_gaps("Resting Heart Rate", ts, T("2026-11-01 00:00")) == []
    assert config_for("Resting Heart Rate") is None


def test_single_missed_sleep_night_is_not_a_gap():
    nights = {date(2026, 10, d): 20 for d in range(15, 28) if d not in (21, 25)}
    assert detect_sleep_gaps(nights, date(2026, 10, 28), min_nights=2) == []


def test_two_missed_sleep_nights_is_a_gap():
    nights = {date(2026, 10, d): 20 for d in range(15, 28) if d not in (21, 22)}
    (gap,) = detect_sleep_gaps(nights, date(2026, 10, 28), min_nights=2)
    assert gap.start == T("2026-10-21 00:00") and gap.end == T("2026-10-23 00:00")


def test_trailing_sleep_gap():
    nights = {date(2026, 10, d): 20 for d in range(15, 21)}
    (gap,) = detect_sleep_gaps(nights, date(2026, 10, 24), min_nights=2)  # 21,22,23 missing
    assert gap.ongoing


def test_outage_metrics_cluster_into_one_candidate():
    now = T("2026-10-26 09:00")
    gaps = []
    for m, (a, b) in OUTAGE.items():
        ts = series("2026-10-23 00:00", "2026-10-26 09:00", 1.0, hole=(a, b))
        gaps += detect_metric_gaps(m, ts, now)
    (cand,) = cluster_gaps(gaps, now)
    assert set(cand.metrics) == set(OUTAGE)


# --------------------------------------------------------------------------
# regression fixtures
# --------------------------------------------------------------------------

def test_walking_gap_22_23_oct_reported_exactly_once(conn):
    full = {m: series("2026-10-15 00:00", "2026-11-09 00:00", 1.0,
                      hole=("2026-10-22 07:55", "2026-10-23 11:10")) for m in WALKING}
    full.update(healthy_continuous())
    h = Harness(conn, full)
    day = T("2026-10-23 12:00")
    while day <= T("2026-11-04 12:00"):          # includes Mondays 26 Oct and 2 Nov
        h.run(day.strftime("%Y-%m-%d %H:%M"))
        day += timedelta(days=1)

    events = ev.list_events(conn)
    assert len(events) == 1                       # grouped quartet, one row
    assert set(events[0].metrics) == set(WALKING)
    assert not events[0].immediate
    assert len(h.emails) == 1                     # digest-only: one email in total...
    assert h.emails[0][1].count("#1 ") == 1       # ...mentioning it once under NEW
    assert h.pushes == []                         # not critical


def test_walking_gap_is_held_until_digest_day(conn):
    full = {m: series("2026-10-15 00:00", "2026-11-09 00:00", 1.0,
                      hole=("2026-10-22 07:55", "2026-10-23 11:10")) for m in WALKING}
    h = Harness(conn, full)
    h.run("2026-10-24 12:00")                     # Saturday
    assert h.emails == [] and h.statuses() == [(1, "confirmed")]
    h.run("2026-10-26 09:00")                     # Monday
    assert len(h.emails) == 1


def test_multi_metric_outage_24_25_oct_is_one_grouped_event(conn):
    full = {m: series("2026-10-15 00:00", "2026-11-09 00:00", 1.0, hole=hole)
            for m, hole in OUTAGE.items()}
    h = Harness(conn, full)

    # Evening of the 25th: trailing/just-closed -> open, silent.
    h.run("2026-10-25 23:30")
    assert h.emails == [] and h.pushes == []
    assert all(s == "open" for _, s in h.statuses())

    h.run("2026-10-26 09:00")                     # feed is back -> confirmed
    events = ev.list_events(conn)
    assert len(events) == 1 and events[0].status == "confirmed"
    assert set(events[0].metrics) == set(OUTAGE)
    assert events[0].critical
    assert len(h.emails) == 1 and len(h.pushes) == 1
    assert "outage" in h.emails[0][1]

    for d in ("2026-10-27 09:00", "2026-10-28 09:00", "2026-10-29 09:00", "2026-11-02 09:00"):
        h.run(d)
    assert len(h.emails) == 1 and len(h.pushes) == 1   # unchanged state: silence


def test_trailing_gap_does_not_alert_until_confirmed(conn):
    last = "2026-11-01 00:00"
    full = {"Step Count": series("2026-10-20 00:00", last, 1.0)}
    h = Harness(conn, full)
    h.run("2026-11-02 12:00")                     # 36h trailing
    assert h.emails == [] and h.statuses() == [(1, "open")]
    h.run("2026-11-03 12:00")                     # 60h - still open
    assert h.emails == [] and h.statuses() == [(1, "open")]
    h.run("2026-11-04 12:00")                     # 84h >= 3 days -> escalated
    assert h.statuses() == [(1, "confirmed")] and len(h.emails) == 1
    h.run("2026-11-05 12:00")
    assert len(h.emails) == 1


def test_single_missed_sleep_day_never_alerts(conn):
    nights = {date(2026, 10, d): 20 for d in range(10, 31) if d != 21}
    h = Harness(conn, {}, sleep_nights=nights)
    for d in range(22, 31):
        h.run(f"2026-10-{d} 09:00")
    assert h.emails == [] and h.pushes == [] and ev.list_events(conn) == []


def test_two_missed_sleep_nights_pushes_once(conn):
    nights = {date(2026, 10, d): 20 for d in range(10, 31) if d not in (21, 22)}
    h = Harness(conn, {}, sleep_nights=nights)
    for d in range(23, 29):
        h.run(f"2026-10-{d} 09:00")
    assert len(h.emails) == 1 and len(h.pushes) == 1


def test_unchanged_state_sends_no_email_on_consecutive_days(conn):
    full = {"Step Count": series("2026-10-15 00:00", "2026-11-09 00:00", 1.0,
                                 hole=("2026-10-22 00:00", "2026-10-24 00:00"))}
    h = Harness(conn, full)
    for d in range(25, 32):
        h.run(f"2026-10-{d} 09:00")
    assert len(h.emails) == 1


# --------------------------------------------------------------------------
# manual export workflow
# --------------------------------------------------------------------------

def _confirmed_step_gap(conn):
    full = {"Step Count": series("2026-10-15 00:00", "2026-11-09 00:00", 1.0,
                                 hole=("2026-10-22 00:00", "2026-10-24 00:00"))}
    h = Harness(conn, full)
    h.run("2026-10-25 09:00")
    assert h.statuses() == [(1, "confirmed")]
    return h, full


def test_export_that_fills_the_gap_resolves_it(conn):
    h, full = _confirmed_step_gap(conn)
    ev.mark_exported(conn, 1, T("2026-10-25 10:00"))
    h.full["Step Count"] = series("2026-10-15 00:00", "2026-11-09 00:00", 1.0)   # backfilled
    h.run("2026-10-26 09:00")
    assert h.statuses() == [(1, "resolved")]
    assert len(h.emails) == 1


def test_export_that_stays_empty_marks_genuinely_missing(conn):
    h, _ = _confirmed_step_gap(conn)
    ev.mark_exported(conn, 1, T("2026-10-25 10:00"))
    h.run("2026-10-26 09:00")
    assert h.statuses() == [(1, "unrecoverable")]
    for d in range(27, 32):                        # never reappears or re-notifies
        h.run(f"2026-10-{d} 09:00")
    assert h.statuses() == [(1, "unrecoverable")] and len(h.emails) == 1


def test_export_not_verified_before_delay(conn):
    h, _ = _confirmed_step_gap(conn)
    ev.mark_exported(conn, 1, T("2026-10-26 08:30"))
    h.run("2026-10-26 09:00")                      # only 30 min later
    assert h.statuses() == [(1, "confirmed")]


def test_exported_older_than_lookback_still_verified(conn):
    """Check-off long after the gap must still be verified (scan window stretches)."""
    h, _ = _confirmed_step_gap(conn)
    ev.mark_exported(conn, 1, T("2026-11-05 09:00"))
    h.run("2026-11-06 09:00")
    assert h.statuses() == [(1, "unrecoverable")]


def test_dismiss_and_state_guards(conn):
    h, _ = _confirmed_step_gap(conn)
    ev.dismiss(conn, 1, T("2026-10-25 10:00"), "watch off")
    assert h.statuses() == [(1, "unrecoverable")]
    with pytest.raises(ev.EventStateError):
        ev.mark_exported(conn, 1, T("2026-10-25 11:00"))
    with pytest.raises(LookupError):
        ev.mark_exported(conn, 99, T("2026-10-25 11:00"))


def test_awaiting_export_listed_while_unchecked(conn):
    h, _ = _confirmed_step_gap(conn)
    report = h.run("2026-10-26 09:00")
    assert [e.id for e in report.awaiting_export] == [1]
    ev.mark_exported(conn, 1, T("2026-10-26 09:30"))
    assert h.run("2026-10-26 09:45").awaiting_export == []
