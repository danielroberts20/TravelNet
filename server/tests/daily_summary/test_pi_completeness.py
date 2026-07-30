"""
test_pi_completeness.py — Tests for the pi_complete presence-based
completeness logic in scheduled_tasks/daily_summary/pi.py.

Previously pi_complete closed blindly 2 days after the date regardless of
whether watchdog/power data had actually arrived, which permanently masked
gaps and dropped straggler data that arrived late (compute_daily_summary_flow
only reprocesses pi_complete = 0 dates). It should now close as soon as data
is present, falling back to a longer calendar-age closure only when nothing
ever arrives.
"""

from datetime import datetime, timedelta, timezone

from scheduled_tasks.daily_summary.pi import PI_DOMAIN, _NO_DATA_FALLBACK_DAYS, _pi_data_present


def _days_ago(n: int) -> str:
    return (datetime.now(timezone.utc).date() - timedelta(days=n)).isoformat()


# ---------------------------------------------------------------------------
# _pi_data_present
# ---------------------------------------------------------------------------

def test_present_with_watchdog_heartbeats():
    assert _pi_data_present({"watchdog_heartbeats_received": 5, "avg_w_pi": None}) is True


def test_present_with_power_only():
    assert _pi_data_present({"watchdog_heartbeats_received": 0, "avg_w_pi": 12.3}) is True


def test_absent_when_both_missing():
    assert _pi_data_present({"watchdog_heartbeats_received": 0, "avg_w_pi": None}) is False
    assert _pi_data_present({"watchdog_heartbeats_received": None, "avg_w_pi": None}) is False


# ---------------------------------------------------------------------------
# PI_DOMAIN.completeness_predicate
# ---------------------------------------------------------------------------

def test_closes_immediately_when_data_present_even_if_very_recent():
    data = {"watchdog_heartbeats_received": 3, "avg_w_pi": None}
    assert PI_DOMAIN.completeness_predicate(_days_ago(0), data, None) is True


def test_stays_open_without_data_before_fallback():
    data = {"watchdog_heartbeats_received": 0, "avg_w_pi": None}
    # Old bug: this used to close blindly after 2 days regardless of data.
    assert PI_DOMAIN.completeness_predicate(_days_ago(2), data, None) is False
    assert PI_DOMAIN.completeness_predicate(_days_ago(_NO_DATA_FALLBACK_DAYS - 1), data, None) is False


def test_closes_after_fallback_even_without_data():
    data = {"watchdog_heartbeats_received": 0, "avg_w_pi": None}
    assert PI_DOMAIN.completeness_predicate(_days_ago(_NO_DATA_FALLBACK_DAYS), data, None) is True


def test_pi_domain_flag_name_unchanged():
    assert PI_DOMAIN.completeness_flag == "pi_complete"
