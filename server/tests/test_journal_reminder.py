"""Journal coverage rules, the quiet-hours model, and the reminder planner."""
import json
import sqlite3
from datetime import date, datetime, timedelta, timezone
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pytest

from database.journal import timing
from database.journal.coverage import counted_days, covered_today, entry_saved_since, missed_streak
from database.journal.table import JournalTable
from scheduled_tasks import journal_reminder as jr

MEL = ZoneInfo("Australia/Melbourne")
# Saturday 2026-11-28 11:00 in Melbourne
NOW = datetime(2026, 11, 28, 0, 0, tzinfo=timezone.utc)
TODAY = date(2026, 11, 28)

SCHEMA = """
CREATE TABLE places (id INTEGER PRIMARY KEY);
CREATE TABLE trigger_log (id INTEGER PRIMARY KEY, trigger TEXT, fired_at TEXT, place_id INTEGER, payload TEXT);
CREATE TABLE transition_timezone (transitioned_at TEXT, to_tz TEXT);
CREATE TABLE health_quantity (timestamp TEXT, metric TEXT, value REAL, source TEXT);
CREATE TABLE location_unified (timestamp TEXT, latitude REAL, longitude REAL, place_id INTEGER);
CREATE TABLE health_sleep (start_ts TEXT, end_ts TEXT, stage TEXT);
CREATE TABLE flights (departed_at TEXT, arrived_at TEXT);
INSERT INTO transition_timezone VALUES ('2026-09-11T00:00:00Z', 'Australia/Melbourne');
"""


@pytest.fixture
def conn(tmp_path):
    path = tmp_path / "t.db"
    c = sqlite3.connect(path); c.executescript(SCHEMA); c.commit(); c.close()

    def get_conn(*a, **k):
        c = sqlite3.connect(path); c.row_factory = sqlite3.Row
        c.execute("PRAGMA foreign_keys = ON;")
        return c

    with patch("database.journal.table.get_conn", get_conn):
        JournalTable().init()
    c = get_conn()
    yield c
    c.close()


def entry(conn, id, written, start=None, end=None, provenance="contemporaneous", created_at=None):
    start = start or written; end = end or start
    ts = datetime.combine(date.fromisoformat(written), datetime.min.time(), tzinfo=MEL) + timedelta(hours=20)
    conn.execute("""INSERT INTO journal_entries (id, ts_utc, local_date, source, provenance, text,
        covers_start, covers_end, created_at) VALUES (?,?,?,?,?,?,?,?,?)""",
        (id, ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), written, "quick", provenance, "t",
         start, end, created_at or ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")))
    conn.commit()


# ---- coverage ---------------------------------------------------------------

def test_counted_days_rules(conn):
    entry(conn, "live", "2026-11-20")
    entry(conn, "retro-ok", "2026-11-22", start="2026-11-20", provenance="retro")   # 2 days late: counts
    entry(conn, "retro-old", "2026-11-26", start="2026-11-15", provenance="retro")  # 11 days late: does not
    entry(conn, "weekly", "2026-11-27", start="2026-11-01", end="2026-11-07", provenance="retro")  # multi-day: all
    entry(conn, "draft", "2026-11-27", start="2026-11-10", provenance="drafted_from_telemetry")
    got = counted_days(conn, date(2026, 11, 1), date(2026, 11, 28), 3)
    assert date(2026, 11, 20) in got
    assert date(2026, 11, 15) not in got
    assert {date(2026, 11, d) for d in range(1, 8)} <= got
    assert date(2026, 11, 10) not in got


def test_missed_streak_counts_back_from_yesterday(conn):
    entry(conn, "a", "2026-11-24")
    assert missed_streak(conn, TODAY, 3) == 3        # 25, 26, 27 missed; today excluded
    entry(conn, "b", "2026-11-27")
    assert missed_streak(conn, TODAY, 3) == 0
    assert covered_today(conn, TODAY, 3) is False
    entry(conn, "c", "2026-11-28")
    assert covered_today(conn, TODAY, 3) is True


def test_entry_saved_since(conn):
    entry(conn, "a", "2026-11-27", created_at="2026-11-27T10:00:00Z")
    assert entry_saved_since(conn, "2026-11-27T09:00:00Z") is True
    assert entry_saved_since(conn, "2026-11-27T11:00:00Z") is False


# ---- timing -------------------------------------------------------------------

def test_tz_at_follows_transitions():
    tr = [(datetime(2026, 9, 8, tzinfo=timezone.utc), ZoneInfo("Pacific/Fiji")),
          (datetime(2026, 9, 11, tzinfo=timezone.utc), MEL)]
    assert timing.tz_at(tr, datetime(2026, 9, 9, tzinfo=timezone.utc)).key == "Pacific/Fiji"
    assert timing.tz_at(tr, datetime(2026, 11, 1, tzinfo=timezone.utc)).key == "Australia/Melbourne"
    assert timing.tz_at(tr, datetime(2026, 1, 1, tzinfo=timezone.utc)).key == "Pacific/Fiji"   # before any: earliest


def test_candidate_hours_respect_wake_and_sleep_buffers():
    b = timing.Bounds(wake_min=7 * 60, onset_min=23 * 60)
    assert timing.candidate_hours(b, 120, 90) == list(range(9, 22))
    assert timing.candidate_hours(timing.Bounds(7 * 60, 25 * 60), 120, 90) == list(range(9, 24))   # onset after midnight


def _profile(quiet_hour, busy_steps=400):
    """Eight weeks of every weekday: busy all day except `quiet_hour`."""
    steps, alive = {}, set()
    for i in range(1, 57):
        d = TODAY - timedelta(days=i)
        for h in range(24):
            alive.add((d, h))
            steps[(d, h)] = 0.0 if h == quiet_hour else busy_steps
    return timing.quiet_profile(steps, alive, TODAY)


COMMON = dict(wake_buffer_min=120, sleep_buffer_min=90, learn_min_entries=8, fallback_hour=20)
B = timing.Bounds(7 * 60, 23 * 60)


def test_quietest_hour_is_chosen():
    now = datetime(2026, 11, 28, 8, 0, tzinfo=MEL)
    slot = timing.pick_today(now, B, _profile(quiet_hour=15), {}, 0, **COMMON)
    assert slot == datetime(2026, 11, 28, 15, 0, tzinfo=MEL)


def test_cold_start_prefers_fallback_hour():
    now = datetime(2026, 11, 28, 8, 0, tzinfo=MEL)
    assert timing.pick_today(now, B, {}, {}, 0, **COMMON).hour == 20


def test_slot_must_be_in_the_future_and_window_can_close():
    now = datetime(2026, 11, 28, 16, 0, tzinfo=MEL)
    slot = timing.pick_today(now, B, _profile(quiet_hour=15), {}, 0, **COMMON)
    assert slot.hour > 16                                    # 15:00 has passed; best remaining hour
    late = datetime(2026, 11, 28, 22, 30, tzinfo=MEL)
    assert timing.pick_today(late, B, {}, {}, 0, **COMMON) is None


def test_writing_habits_blend_in_only_after_enough_entries():
    now = datetime(2026, 11, 28, 8, 0, tzinfo=MEL)
    quiet = _profile(quiet_hour=15)
    writing = {(5, 18): 5.0}                                 # Saturdays at 18:00
    assert timing.pick_today(now, B, quiet, writing, 3, **COMMON).hour == 15      # too few entries: ignored
    assert timing.pick_today(now, B, quiet, writing, 12, **COMMON).hour in (15, 18)
    flat = timing.pick_today(now, B, {}, writing, 12, **COMMON)
    assert flat.hour == 18                                   # no activity data: writing habit decides


def test_weekly_slot_respects_earliest_date_and_picks_a_good_weekday():
    now = datetime(2026, 11, 28, 9, 0, tzinfo=MEL)
    slot = timing.pick_weekly(now, date(2026, 12, 2), B, {}, {}, 0, days_ahead=7, **COMMON)
    assert slot.date() >= date(2026, 12, 2)
    assert timing.pick_weekly(now, date(2027, 1, 1), B, {}, {}, 0, days_ahead=7, **COMMON) is None


def test_transit_day_detection(conn):
    assert timing.is_transit_day(conn, "2026-11-28T00:00:00Z", "2026-11-29T00:00:00Z") is False
    conn.execute("INSERT INTO flights VALUES ('2026-11-28T03:00:00Z', '2026-11-28T09:00:00Z')"); conn.commit()
    assert timing.is_transit_day(conn, "2026-11-28T00:00:00Z", "2026-11-29T00:00:00Z") is True
    assert timing.is_transit_day(conn, "2026-11-20T00:00:00Z", "2026-11-21T00:00:00Z") is False


def test_sleep_bounds_fallback_without_data(conn):
    assert timing.load_sleep_bounds(conn, NOW, timing.load_transitions(conn)) == timing.FALLBACK_BOUNDS


def test_activity_only_counts_hours_when_the_phone_was_alive(conn):
    conn.execute("INSERT INTO location_unified VALUES ('2026-11-27T05:10:00Z', 0, 0, NULL)")   # 16:10 Melbourne (AEDT)
    conn.execute("INSERT INTO health_quantity VALUES ('2026-11-27T05:20:00Z','Step Count',300,'Watch')")
    conn.execute("INSERT INTO health_quantity VALUES ('2026-11-27T05:20:00Z','Step Count',300,'iPhone')")   # dup
    conn.commit()
    tr = timing.load_transitions(conn)
    steps, alive = timing.load_activity(conn, NOW - timedelta(days=3), NOW, tr)
    assert alive == {(date(2026, 11, 27), 16)} and steps[(date(2026, 11, 27), 16)] == 300


# ---- planner ----------------------------------------------------------------

def plan(conn, now=NOW):
    return jr.plan_reminder(conn, now)


def test_daily_mode_schedules_a_slot_in_the_future(conn):
    entry(conn, "a", "2026-11-27")
    p = plan(conn)
    assert (p["mode"], p["action"]) == ("daily", "schedule")
    assert p["when"] >= NOW + timedelta(minutes=10) and p["when"].date() == TODAY
    assert p["when"].hour == 20 and "wake" in p["reason"]        # cold start → fallback hour


def test_no_daily_nudge_when_today_is_done_or_already_nudged_or_pending(conn):
    entry(conn, "a", "2026-11-27"); entry(conn, "b", "2026-11-28")
    assert plan(conn)["reason"] == "today already journalled"

    conn2 = conn
    conn2.execute("DELETE FROM journal_entries WHERE id = 'b'")
    conn2.execute("INSERT INTO trigger_log (trigger, fired_at) VALUES ('journal_nudge_daily', '2026-11-27T20:00:00Z')")
    conn2.commit()
    assert plan(conn)["reason"] == "already nudged today"       # 11-27 20:00Z = 11-28 07:00 local

    conn2.execute("DELETE FROM trigger_log")
    jr._log(conn2, jr.T_SCHEDULED, NOW - timedelta(hours=1),
            {"mode": "daily", "scheduled_at": (NOW + timedelta(hours=8)).isoformat()}); conn2.commit()
    assert "already scheduled" in plan(conn)["reason"]


def test_transit_day_skips_daily_nudge(conn):
    entry(conn, "a", "2026-11-27")
    conn.execute("INSERT INTO flights VALUES ('2026-11-27T20:00:00Z', '2026-11-28T02:00:00Z')"); conn.commit()
    assert plan(conn)["reason"] == "flight or timezone-change day"


def test_three_missed_days_switch_to_backlog(conn):
    entry(conn, "a", "2026-11-24")                        # 25, 26, 27 missed
    p = plan(conn)
    assert (p["mode"], p["streak"], p["action"]) == ("backlog", 3, "schedule")
    assert TODAY <= p["when"].date() < TODAY + timedelta(days=7)


def test_backlog_respects_the_weekly_interval(conn):
    entry(conn, "a", "2026-11-24")
    conn.execute("INSERT INTO trigger_log (trigger, fired_at) VALUES ('journal_nudge_backlog', '2026-11-26T09:00:00Z')")
    conn.commit()
    p = plan(conn)                                         # last was 11-26 local: next no earlier than 12-02
    assert p["action"] == "schedule" and p["when"].astimezone(MEL).date() >= date(2026, 12, 2)


def test_should_send_rechecks_at_fire_time(conn):
    entry(conn, "a", "2026-11-27")
    planned = "2026-11-28T00:00:00Z"
    now = NOW + timedelta(hours=9)
    assert jr.should_send(conn, "daily", planned, now) == (True, "ok")

    assert jr.should_send(conn, "backlog", planned, now)[1].startswith("mode changed")
    entry(conn, "b", "2026-11-28", created_at="2026-11-28T05:00:00Z")
    assert "entry was saved" in jr.should_send(conn, "daily", planned, now)[1]
    assert jr.should_send(conn, "daily", "2026-11-28T06:00:00Z", now)[1] == "today already journalled"

    conn.execute("DELETE FROM journal_entries WHERE id = 'b'")
    conn.execute("INSERT INTO trigger_log (trigger, fired_at) VALUES ('journal_nudge_daily', '2026-11-28T03:00:00Z')")
    conn.commit()
    assert jr.should_send(conn, "daily", planned, now) == (False, "already nudged today")
