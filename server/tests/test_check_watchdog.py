# tests/test_check_watchdog.py

from datetime import datetime, timezone, timedelta
from unittest.mock import patch

import pytest

from scheduled_tasks import check_watchdog
from scheduled_tasks.check_watchdog import evaluate_staleness

@pytest.fixture(autouse=True)
def _patch_run_logger():
    with patch("scheduled_tasks.check_watchdog.get_run_logger"):
        yield


NOW = datetime(2026, 10, 7, 12, 0, 0, tzinfo=timezone.utc)


def _hb(minutes_ago: float) -> dict:
    ts = (NOW - timedelta(minutes=minutes_ago)).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {"received_at": ts, "consecutive_failures": 0}


# --- evaluate_staleness ---

def test_no_heartbeat_ever_is_unhealthy():
    healthy, detail = evaluate_staleness(None, now=NOW)
    assert healthy is False
    assert detail == "no heartbeat ever received"


def test_recent_heartbeat_is_healthy():
    healthy, detail = evaluate_staleness(_hb(3), now=NOW)
    assert healthy is True
    assert detail == "last seen 180s ago"


def test_heartbeat_exactly_at_threshold_is_still_healthy():
    healthy, _ = evaluate_staleness(_hb(10), now=NOW)
    assert healthy is True


def test_stale_heartbeat_is_unhealthy():
    healthy, detail = evaluate_staleness(_hb(11), now=NOW)
    assert healthy is False
    assert detail == "last seen 660s ago"


def test_custom_threshold():
    assert evaluate_staleness(_hb(4), threshold_minutes=3, now=NOW)[0] is False
    assert evaluate_staleness(_hb(4), threshold_minutes=5, now=NOW)[0] is True


# --- flow ---

def test_flow_skips_check_while_server_just_started():
    with patch.object(check_watchdog, "get_server_uptime", return_value=120.0), \
         patch.object(check_watchdog, "get_last_heartbeat") as hb, \
         patch.object(check_watchdog, "error_notification") as notify:
        check_watchdog.check_watchdog_flow.fn()
    hb.assert_not_called()
    notify.assert_not_called()


def test_flow_notifies_when_stale():
    with patch.object(check_watchdog, "get_server_uptime", return_value=7200.0), \
         patch.object(check_watchdog, "get_last_heartbeat", return_value=None), \
         patch.object(check_watchdog, "error_notification") as notify:
        check_watchdog.check_watchdog_flow.fn()
    notify.assert_called_once()
    assert "no heartbeat ever received" in notify.call_args.args[0]


def test_flow_is_silent_when_healthy():
    fresh = {"received_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
             "consecutive_failures": 0}
    with patch.object(check_watchdog, "get_server_uptime", return_value=7200.0), \
         patch.object(check_watchdog, "get_last_heartbeat", return_value=fresh), \
         patch.object(check_watchdog, "error_notification") as notify:
        check_watchdog.check_watchdog_flow.fn()
    notify.assert_not_called()
