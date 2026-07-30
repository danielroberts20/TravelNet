"""
test_base.py — Tests for scheduled_tasks/daily_summary/base.py completeness
predicates and the Domain dataclass's is_closed() dispatch.
"""

from datetime import datetime, timedelta, timezone

from scheduled_tasks.daily_summary.base import (
    Domain,
    closed_after,
    closed_when_present_else_after,
    never_auto_close,
)


def _days_ago(n: int) -> str:
    return (datetime.now(timezone.utc).date() - timedelta(days=n)).isoformat()


# ---------------------------------------------------------------------------
# closed_after
# ---------------------------------------------------------------------------

def test_closed_after_true_when_old_enough():
    predicate = closed_after(2)
    assert predicate(_days_ago(2), {}, None) is True
    assert predicate(_days_ago(5), {}, None) is True


def test_closed_after_false_when_too_recent():
    predicate = closed_after(2)
    assert predicate(_days_ago(0), {}, None) is False
    assert predicate(_days_ago(1), {}, None) is False


def test_closed_after_ignores_data_and_conn():
    predicate = closed_after(2)
    assert predicate(_days_ago(3), {"anything": "irrelevant"}, "not-a-real-conn") is True


# ---------------------------------------------------------------------------
# never_auto_close
# ---------------------------------------------------------------------------

def test_never_auto_close_always_false():
    assert never_auto_close(_days_ago(10_000), {}, None) is False
    assert never_auto_close(_days_ago(0), {"x": 1}, None) is False


# ---------------------------------------------------------------------------
# closed_when_present_else_after
# ---------------------------------------------------------------------------

def test_closed_when_present_closes_immediately_regardless_of_age():
    predicate = closed_when_present_else_after(14, lambda data: bool(data.get("has_data")))
    assert predicate(_days_ago(0), {"has_data": True}, None) is True


def test_closed_when_present_stays_open_before_fallback_without_data():
    predicate = closed_when_present_else_after(14, lambda data: bool(data.get("has_data")))
    assert predicate(_days_ago(1), {"has_data": False}, None) is False
    assert predicate(_days_ago(13), {"has_data": False}, None) is False


def test_closed_when_present_closes_after_fallback_without_data():
    predicate = closed_when_present_else_after(14, lambda data: bool(data.get("has_data")))
    assert predicate(_days_ago(14), {"has_data": False}, None) is True
    assert predicate(_days_ago(30), {"has_data": False}, None) is True


# ---------------------------------------------------------------------------
# Domain.is_closed dispatch
# ---------------------------------------------------------------------------

def test_domain_is_closed_forwards_local_date_data_and_conn():
    calls = []

    def predicate(local_date, data, conn):
        calls.append((local_date, data, conn))
        return True

    domain = Domain(
        name="test",
        columns=frozenset(),
        completeness_flag="test_complete",
        compute_fn=lambda conn, ctx: {},
        completeness_predicate=predicate,
    )

    sentinel_conn = object()
    result = domain.is_closed("2026-01-01", {"a": 1}, sentinel_conn)

    assert result is True
    assert calls == [("2026-01-01", {"a": 1}, sentinel_conn)]
