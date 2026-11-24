"""Tests for health_gaps/recover.py: the recovery run, with every external effect faked."""

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import pytest

from database.health.gap_events.table import init_schema
from health_gaps import events as ev
from health_gaps import recover as rc
from health_gaps.hae_client import HAEAuthError, HAEProtocolError, HAEUnreachable
from health_gaps.recovery import WaitResult

UTC = timezone.utc
NOW = datetime(2026, 11, 24, 12, 0, tzinfo=UTC)


def T(s):
    return datetime.strptime(s, "%Y-%m-%d %H:%M").replace(tzinfo=UTC)


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    init_schema(c)
    return c


def add_event(conn, start, end, metrics=("heart_rate",), status="confirmed", **cols):
    m = {x: [start.strftime("%Y-%m-%dT%H:%M:%SZ"), end.strftime("%Y-%m-%dT%H:%M:%SZ") if end else None] for x in metrics}
    cur = conn.execute(
        "INSERT INTO health_gap_events (group_key, metrics, window_start, window_end, status, first_seen, last_seen) "
        "VALUES ('g', ?, ?, ?, ?, 'x', 'x')",
        (json.dumps(m), start.strftime("%Y-%m-%dT%H:%M:%SZ"),
         end.strftime("%Y-%m-%dT%H:%M:%SZ") if end else None, status))
    for k, v in cols.items():
        conn.execute(f"UPDATE health_gap_events SET {k} = ? WHERE id = ?", (v, cur.lastrowid))
    conn.commit()
    return cur.lastrowid


def pts(n):
    return {"metrics": [{"name": "x", "units": "u", "data": [{"date": "d"}] * n}]}


class FakeClient:
    def __init__(self, default=None, script=None, connect_error=None):
        self.default = pts(0) if default is None else default
        self.script, self.connect_error, self.calls = script or {}, connect_error, []

    def get_health_metrics(self, metric, start, end, **_):
        self.calls.append((metric, start, end))
        item = self.script.get(len(self.calls) - 1, self.default)
        if isinstance(item, Exception):
            raise item
        return item


class Run:
    """Wires fakes into RecoveryDeps and records what happened."""
    def __init__(self, conn, *, up=False, wait_up=True, client=None, connect_error=None, now=NOW,
                 rescan_effect=None):
        self.conn, self.log = conn, []
        self.client = client or FakeClient()
        self.ingested, self.finished, self.prompted = [], [], []
        self.rescans = 0

        @contextmanager
        def make_client():
            if connect_error:
                raise connect_error
            yield self.client

        def wait(notify):
            notify()
            return WaitResult(wait_up, True, 12.0, 12)

        def rescan(_now):
            self.rescans += 1
            if rescan_effect:
                rescan_effect(conn)

        self.deps = rc.RecoveryDeps(
            now=lambda: now, server_up=lambda: up, wait_for_server=wait,
            prompt=lambda events: self.prompted.append([e.id for e in events]),
            make_client=make_client, ingest=self.ingested.append, rescan=rescan,
            finish=lambda t, b: self.finished.append((t, b)),
        )

    def go(self, **params):
        return rc.run_recovery(self.conn, self.deps, rc.RecoveryParams(**params))


def set_status(status, ids=None):
    def effect(conn):
        for i in (ids or [r[0] for r in conn.execute("SELECT id FROM health_gap_events")]):
            conn.execute("UPDATE health_gap_events SET status = ? WHERE id = ?", (status, i))
        conn.commit()
    return effect


def status_of(conn, i):
    return ev.get_event(conn, i).status


# --- gating -----------------------------------------------------------------

def test_nothing_to_do_sends_nothing(conn):
    add_event(conn, T("2026-11-20 00:00"), T("2026-11-21 00:00"), status="resolved")
    r = Run(conn)
    assert r.go().status == rc.NOTHING_TO_DO and r.prompted == [] and r.finished == []


def test_recent_prompt_is_respected_but_force_overrides(conn):
    add_event(conn, T("2026-11-20 00:00"), T("2026-11-21 00:00"), last_prompted_at="2026-11-24T06:00:00Z")
    r = Run(conn)
    assert r.go().status == rc.COOLDOWN and r.prompted == [] and r.client.calls == []
    r2 = Run(conn, rescan_effect=set_status("unrecoverable"))
    assert r2.go(force=True).status == rc.DONE and r2.prompted


def test_prompt_marks_pending_events_so_cooldown_applies_next_time(conn):
    add_event(conn, T("2026-11-20 00:00"), T("2026-11-21 00:00"))
    Run(conn, wait_up=False).go()
    assert ev.get_event(conn, 1).last_prompted_at is not None
    assert Run(conn).go().status == rc.COOLDOWN


def test_server_never_comes_up_is_timeout_and_pulls_nothing(conn):
    add_event(conn, T("2026-11-20 00:00"), T("2026-11-21 00:00"))
    r = Run(conn, wait_up=False)
    out = r.go()
    assert out.status == rc.TIMEOUT and r.client.calls == [] and r.rescans == 0
    assert status_of(conn, 1) == "confirmed" and ev.get_event(conn, 1).pull_attempts == 0
    title, body = r.finished[0]                           # the user is told, and promised a retry
    assert "Timed Out" in title and "I'll try again tomorrow" in body
    assert "safely stop" not in body


def test_server_already_up_skips_the_notification(conn):
    add_event(conn, T("2026-11-20 00:00"), T("2026-11-21 00:00"))
    r = Run(conn, up=True, rescan_effect=set_status("unrecoverable"))
    r.go()
    assert r.prompted == [] and r.client.calls


# --- digest-only gaps --------------------------------------------------------

def test_sparse_gaps_never_start_a_prompt_even_on_digest_day(conn):
    add_event(conn, T("2026-11-20 00:00"), T("2026-11-21 00:00"), metrics=("VO2 Max",), immediate=0)
    r = Run(conn)
    assert r.go(digest_weekday=NOW.weekday()).status == rc.DIGEST_ONLY and r.prompted == []


def test_walking_gaps_prompt_only_on_the_digest_day(conn):
    add_event(conn, T("2026-11-20 00:00"), T("2026-11-21 00:00"), metrics=("Walking Speed",), immediate=0)
    quiet = Run(conn)
    assert quiet.go(digest_weekday=(NOW.weekday() + 1) % 7).status == rc.DIGEST_ONLY and quiet.prompted == []
    assert Run(conn).go().status == rc.DIGEST_ONLY                     # no digest day configured
    day = Run(conn, rescan_effect=set_status("unrecoverable"))
    assert day.go(digest_weekday=NOW.weekday()).status == rc.DONE and day.prompted


def test_digest_day_walking_gap_respects_the_cooldown(conn):
    add_event(conn, T("2026-11-20 00:00"), T("2026-11-21 00:00"), metrics=("Walking Speed",), immediate=0,
              last_prompted_at="2026-11-24T06:00:00Z")
    assert Run(conn).go(digest_weekday=NOW.weekday()).status == rc.COOLDOWN


def test_worth_prompting_rules():
    from health_gaps.recover import worth_prompting
    def e(metrics, immediate):
        return type("E", (), {"metrics": {m: None for m in metrics}, "immediate": immediate})()
    assert worth_prompting(e(["Step Count"], True), False)
    assert not worth_prompting(e(["Walking Speed"], False), False)
    assert worth_prompting(e(["Walking Speed"], False), True)
    assert not worth_prompting(e(["VO2 Max"], False), True)
    assert worth_prompting(e(["VO2 Max", "Walking Speed"], False), True)        # one non-sparse metric is enough
    assert not worth_prompting(e(["Respiratory Rate", "Apple Sleeping Wrist Temperature"], False), True)


def test_digest_only_gaps_never_start_a_prompt(conn):
    add_event(conn, T("2026-11-20 00:00"), T("2026-11-21 00:00"), metrics=("VO2 Max",), immediate=0)
    r = Run(conn)
    out = r.go()
    assert out.status == rc.DIGEST_ONLY and r.prompted == [] and r.client.calls == [] and r.finished == []
    assert ev.get_event(conn, 1).last_prompted_at is None


def test_digest_only_gaps_ride_along_when_an_immediate_gap_prompts(conn):
    imm = add_event(conn, T("2026-11-20 00:00"), T("2026-11-21 00:00"))
    dig = add_event(conn, T("2026-11-18 00:00"), T("2026-11-19 00:00"), metrics=("VO2 Max",), immediate=0)
    r = Run(conn, rescan_effect=set_status("unrecoverable"))
    out = r.go()
    assert sorted(r.prompted[0]) == [imm, dig] and sorted(out.pulled) == [imm, dig]


def test_digest_only_gaps_are_recovered_by_a_forced_manual_run(conn):
    add_event(conn, T("2026-11-20 00:00"), T("2026-11-21 00:00"), metrics=("VO2 Max",), immediate=0)
    r = Run(conn, rescan_effect=set_status("unrecoverable"))
    assert r.go(force=True).status == rc.DONE and r.prompted


def test_digest_only_gap_does_not_reprompt_while_an_immediate_one_is_in_cooldown(conn):
    add_event(conn, T("2026-11-20 00:00"), T("2026-11-21 00:00"), last_prompted_at="2026-11-24T06:00:00Z")
    add_event(conn, T("2026-11-18 00:00"), T("2026-11-19 00:00"), metrics=("VO2 Max",), immediate=0)
    assert Run(conn).go().status == rc.COOLDOWN


# --- outcomes -----------------------------------------------------------------

def test_recovered_gap_is_ingested_and_reported(conn):
    add_event(conn, T("2026-11-20 00:00"), T("2026-11-21 00:00"))
    r = Run(conn, client=FakeClient(default=pts(5)), rescan_effect=set_status("resolved"))
    out = r.go()
    assert out.status == rc.DONE and out.recovered == [1] and out.points > 0
    assert r.ingested and r.rescans == 1
    assert ev.get_event(conn, 1).pulled_at is not None
    title, body = r.finished[0]
    assert "finished" in title.lower() and "1 recovered" in body and "safely stop the HAE server" in body


def test_empty_complete_pull_reports_genuinely_missing(conn):
    add_event(conn, T("2026-11-20 00:00"), T("2026-11-21 00:00"))
    r = Run(conn, rescan_effect=set_status("unrecoverable"))
    out = r.go()
    assert out.missing == [1] and "1 genuinely missing" in r.finished[0][1]


def test_dropped_connection_is_interrupted_and_never_marks_pulled(conn):
    add_event(conn, T("2026-11-20 00:00"), T("2026-11-21 00:00"))
    r = Run(conn, client=FakeClient(script={0: HAEUnreachable("gone")}))
    out = r.go()
    e = ev.get_event(conn, 1)
    assert out.status == rc.INTERRUPTED and e.pulled_at is None and e.pull_attempts == 1
    assert e.status == "confirmed"
    assert "interrupted" in r.finished[0][0].lower() and "retried" in r.finished[0][1]
    assert [x.id for x in ev.needing_recovery(conn)] == [1]


def test_drop_stops_the_run_and_leaves_later_events_untouched(conn):
    newer = add_event(conn, T("2026-11-22 00:00"), T("2026-11-23 00:00"))
    older = add_event(conn, T("2026-11-18 00:00"), T("2026-11-19 00:00"))
    r = Run(conn, client=FakeClient(script={0: HAEUnreachable("gone")}))
    out = r.go()
    assert out.pulled == [newer]                       # newest first, then stop
    assert ev.get_event(conn, older).pull_attempts == 0


def test_newest_event_is_pulled_first(conn):
    old = add_event(conn, T("2026-11-18 00:00"), T("2026-11-19 00:00"))
    new = add_event(conn, T("2026-11-22 00:00"), T("2026-11-23 00:00"))
    out = Run(conn, rescan_effect=set_status("unrecoverable")).go()
    assert out.pulled == [new, old]


def test_connect_failure_is_interrupted_not_missing(conn):
    add_event(conn, T("2026-11-20 00:00"), T("2026-11-21 00:00"))
    r = Run(conn, connect_error=HAEUnreachable("refused"))
    out = r.go()
    assert out.status == rc.INTERRUPTED and ev.get_event(conn, 1).pulled_at is None and r.rescans == 0


def test_bad_token_aborts_with_a_clear_message(conn):
    add_event(conn, T("2026-11-20 00:00"), T("2026-11-21 00:00"))
    r = Run(conn, connect_error=HAEAuthError("401"))
    out = r.go()
    assert out.status == rc.AUTH_ERROR and "token" in r.finished[0][1].lower()
    assert ev.get_event(conn, 1).pull_attempts == 0


def test_protocol_error_on_a_metric_leaves_the_event_pending(conn):
    add_event(conn, T("2026-11-20 00:00"), T("2026-11-21 00:00"))
    r = Run(conn, client=FakeClient(script={0: HAEProtocolError("junk")}))
    out = r.go()
    e = ev.get_event(conn, 1)
    assert out.status == rc.DONE and e.pulled_at is None and e.pull_attempts == 1   # incomplete, retried later
    assert out.pending == [1]


def test_ongoing_gap_is_pulled_once_then_not_reprompted(conn):
    add_event(conn, T("2026-11-20 00:00"), None)
    r = Run(conn)
    assert r.go().status == rc.DONE and ev.get_event(conn, 1).pulled_at is not None
    assert Run(conn).go().status == rc.NOTHING_TO_DO


# --- messages -----------------------------------------------------------------

def test_prompt_text_pluralises():
    one = rc.prompt_text([object()])[1]
    two = rc.prompt_text([object(), object()])[1]
    assert "1 health gap can" in one and "2 health gaps can" in two and "unlocked" in one


def test_timeout_message_uses_the_wait_in_minutes():
    t, b = rc.finish_text(rc.RecoveryOutcome(rc.TIMEOUT, waited_s=1800))
    assert t.startswith("🩺") and b.startswith("⏱️") and "30 min" in b and "try again tomorrow" in b


def test_messages_follow_the_notification_emoji_convention():
    """Topical emoji leads the title, a status emoji leads the body (as in the existing notifications)."""
    assert rc.prompt_text([object()])[0].startswith("🩺")
    done = rc.finish_text(rc.RecoveryOutcome(rc.DONE, recovered=[1]))
    assert done[0].startswith("🩺") and done[1].startswith("✅")
    assert rc.finish_text(rc.RecoveryOutcome(rc.INTERRUPTED))[1].startswith("⚠️")
    for status in (rc.AUTH_ERROR, rc.ERROR):
        t, b = rc.finish_text(rc.RecoveryOutcome(status))
        assert t.startswith("🩺") and b.startswith("❌")


def test_finish_text_never_says_stop_after_an_interruption():
    t, b = rc.finish_text(rc.RecoveryOutcome(rc.INTERRUPTED, pending=[1]))
    assert "safely stop" not in b and "Check that the HAE server is stopped" in b
