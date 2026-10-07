# tests/test_get_fx_up_to_date.py
#
# These tests never use the Prefect engine. Running a flow or task through the
# engine needs a Prefect API, and the developer's profile points at the production
# server (the root conftest.py now blocks that). Instead:
#   * tasks and flows are called through `.fn` (the plain function underneath),
#   * `get_run_logger` is replaced by a standard logger so `caplog` still sees app logs,
#   * for flow tests, the task objects the flow body calls are swapped for their `.fn`,
#     and `record_flow_result` is replaced by a mock.
# What the engine adds (retries, failure hooks) is covered by asserting the wiring.

import logging
import pytest
from datetime import date, timedelta
from unittest.mock import patch, MagicMock

from scheduled_tasks import get_fx_up_to_date as fx
from scheduled_tasks.get_fx_up_to_date import get_fx_up_to_date_flow, get_missing_fx_dates
from notifications import notify_on_completion, log_on_success

MODULE = "scheduled_tasks.get_fx_up_to_date"


# --- Sample data ---

SAMPLE_RESPONSE = {
    "success": True,
    "timeframe": True,
    "start_date": "2026-02-01",
    "end_date": "2026-02-03",
    "source": "GBP",
    "quotes": {
        "2026-02-01": {"GBPUSD": 1.36, "GBPAUD": 1.97},
        "2026-02-02": {"GBPUSD": 1.37, "GBPAUD": 1.96},
        "2026-02-03": {"GBPUSD": 1.38, "GBPAUD": 1.95},
    }
}

ERROR_RESPONSE = {
    "success": False,
    "error": {"type": "invalid_access_key", "info": "Invalid key"}
}


@pytest.fixture(autouse=True)
def plain_logger(caplog):
    """Use a standard logger instead of Prefect's run logger (which needs an engine run)."""
    caplog.set_level(logging.INFO, logger=MODULE)
    with patch(f"{MODULE}.get_run_logger", return_value=logging.getLogger(MODULE)):
        yield


@pytest.fixture
def run_flow():
    """Make the flow body runnable without the engine.

    Swaps the task objects the flow calls for their plain functions and mocks
    record_flow_result. Yields that mock.
    """
    with patch(f"{MODULE}.record_flow_result") as record, \
         patch.object(fx, "check_fx_api_quota", fx.check_fx_api_quota.fn), \
         patch.object(fx, "get_missing_fx_dates", fx.get_missing_fx_dates.fn), \
         patch.object(fx, "fetch_fx_timeframe", fx.fetch_fx_timeframe.fn), \
         patch.object(fx, "store_fx_and_backup", fx.store_fx_and_backup.fn):
        yield record


# --- _get_missing_dates (via the get_missing_fx_dates task) ---

def test_get_missing_dates_finds_gaps():
    """Should return dates present in expected range but absent from DB."""
    mock_conn = MagicMock()
    mock_conn.__enter__ = lambda s: mock_conn
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_conn.execute.return_value.fetchall.return_value = [
        {"date": "2026-02-01"},
        {"date": "2026-02-03"},  # 2026-02-02 is missing
    ]
    with patch("scheduled_tasks.get_fx_up_to_date.get_conn", return_value=mock_conn):
        result = get_missing_fx_dates.fn(date(2026, 2, 3))
    assert result == ["2026-02-02"]


def test_get_missing_dates_no_gaps():
    """Should return empty list when all dates are present."""
    mock_conn = MagicMock()
    mock_conn.__enter__ = lambda s: mock_conn
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_conn.execute.return_value.fetchall.return_value = [
        {"date": "2026-02-01"},
        {"date": "2026-02-02"},
        {"date": "2026-02-03"},
    ]
    with patch("scheduled_tasks.get_fx_up_to_date.get_conn", return_value=mock_conn):
        result = get_missing_fx_dates.fn(date(2026, 2, 3))
    assert result == []


def test_get_missing_dates_empty_db(caplog):
    """Should return empty list and log a warning when DB has no FX data."""
    mock_conn = MagicMock()
    mock_conn.__enter__ = lambda s: mock_conn
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_conn.execute.return_value.fetchall.return_value = []
    with patch("scheduled_tasks.get_fx_up_to_date.get_conn", return_value=mock_conn):
        with caplog.at_level("WARNING", logger="scheduled_tasks.get_fx_up_to_date"):
            result = get_missing_fx_dates.fn(date(2026, 2, 3))
    assert result == []
    assert "No existing FX data" in caplog.text


# --- get_fx_up_to_date ---

@pytest.fixture
def mock_missing_dates():
    """Patch _get_missing_dates to return two missing dates."""
    with patch("scheduled_tasks.get_fx_up_to_date._get_missing_dates",
               return_value=["2026-02-02", "2026-02-03"]) as m:
        yield m


@pytest.fixture
def full_quota():
    """Patch get_api_usage to return full quota (0 used)."""
    with patch("scheduled_tasks.get_fx_up_to_date.get_api_usage",
               return_value={"count": 0}) as m:
        yield m


@pytest.fixture
def no_quota():
    """Patch get_api_usage to return exhausted quota."""
    with patch("scheduled_tasks.get_fx_up_to_date.get_api_usage",
               return_value={"count": 100}) as m:
        yield m


def test_aborts_when_no_quota_remaining(no_quota, run_flow):
    """Should abort before doing anything else when quota is exhausted."""
    with patch("scheduled_tasks.get_fx_up_to_date._get_missing_dates") as missing:
        with pytest.raises(RuntimeError, match="No API quota remaining"):
            get_fx_up_to_date_flow.fn(date(2026, 2, 3))
    missing.assert_not_called()
    run_flow.assert_not_called()


def test_unknown_quota_also_aborts(run_flow):
    with patch("scheduled_tasks.get_fx_up_to_date.get_api_usage", return_value={"count": None}):
        with pytest.raises(RuntimeError, match="Could not verify API quota"):
            get_fx_up_to_date_flow.fn(date(2026, 2, 3))


def test_does_nothing_when_no_missing_dates(full_quota, run_flow, caplog):
    """Should exit early, log info, and record an empty result when no dates are missing."""
    with patch("scheduled_tasks.get_fx_up_to_date._get_missing_dates", return_value=[]), \
         patch("scheduled_tasks.get_fx_up_to_date.requests.get") as mock_get:
        result = get_fx_up_to_date_flow.fn(date(2026, 2, 3))
    assert "nothing to do" in caplog.text
    assert result == {"start_date": "", "end_date": "", "dates_inserted": 0, "backup_path": ""}
    run_flow.assert_called_once_with(result)
    mock_get.assert_not_called()


def test_successful_backfill(full_quota, mock_missing_dates, run_flow):
    """Should call API, increment usage, insert quotes, and save backup."""
    with patch("scheduled_tasks.get_fx_up_to_date.requests.get") as mock_get, \
         patch("scheduled_tasks.get_fx_up_to_date.increment_api_usage") as mock_increment, \
         patch("scheduled_tasks.get_fx_up_to_date.insert_fx_json") as mock_insert, \
         patch("builtins.open", MagicMock()), \
         patch("scheduled_tasks.get_fx_up_to_date.json.dump"):
        mock_get.return_value.json.return_value = SAMPLE_RESPONSE
        result = get_fx_up_to_date_flow.fn(date(2026, 2, 3))

    mock_increment.assert_called_once_with("exchangerate.host")
    mock_insert.assert_called_once_with(SAMPLE_RESPONSE["quotes"])
    assert (result["start_date"], result["end_date"], result["dates_inserted"]) == ("2026-02-02", "2026-02-03", 3)
    run_flow.assert_called_once_with(result)


def test_requests_exactly_the_missing_range(full_quota, mock_missing_dates, run_flow):
    with patch("scheduled_tasks.get_fx_up_to_date.requests.get") as mock_get, \
         patch("scheduled_tasks.get_fx_up_to_date.increment_api_usage"), \
         patch("scheduled_tasks.get_fx_up_to_date.insert_fx_json"), \
         patch("builtins.open", MagicMock()), \
         patch("scheduled_tasks.get_fx_up_to_date.json.dump"):
        mock_get.return_value.json.return_value = SAMPLE_RESPONSE
        get_fx_up_to_date_flow.fn(date(2026, 2, 3))
    params = mock_get.call_args.kwargs["params"]
    assert (params["start_date"], params["end_date"]) == ("2026-02-02", "2026-02-03")


def test_increments_usage_even_on_api_error(full_quota, mock_missing_dates, run_flow):
    """Should increment usage counter even when API returns an error."""
    with patch("scheduled_tasks.get_fx_up_to_date.requests.get") as mock_get, \
         patch("scheduled_tasks.get_fx_up_to_date.increment_api_usage") as mock_increment, \
         patch("scheduled_tasks.get_fx_up_to_date.insert_fx_json") as mock_insert, \
         pytest.raises(RuntimeError, match="API error"):
        mock_get.return_value.json.return_value = ERROR_RESPONSE
        get_fx_up_to_date_flow.fn(date(2026, 2, 3))

    mock_increment.assert_called_once_with("exchangerate.host")
    mock_insert.assert_not_called()
    run_flow.assert_not_called()


def test_api_error_is_raised_with_the_api_message(full_quota, mock_missing_dates, run_flow):
    """The API's own error must reach the exception (the engine logs it and fires the failure hook)."""
    with patch("scheduled_tasks.get_fx_up_to_date.requests.get") as mock_get, \
         patch("scheduled_tasks.get_fx_up_to_date.increment_api_usage"), \
         pytest.raises(RuntimeError) as exc:
        mock_get.return_value.json.return_value = ERROR_RESPONSE
        get_fx_up_to_date_flow.fn(date(2026, 2, 3))
    assert "API error" in str(exc.value) and "Invalid key" in str(exc.value)


def test_empty_quotes_response_is_an_error(full_quota, mock_missing_dates, run_flow):
    with patch("scheduled_tasks.get_fx_up_to_date.requests.get") as mock_get, \
         patch("scheduled_tasks.get_fx_up_to_date.increment_api_usage"), \
         patch("scheduled_tasks.get_fx_up_to_date.insert_fx_json") as mock_insert, \
         pytest.raises(RuntimeError, match="No quotes returned"):
        mock_get.return_value.json.return_value = {"success": True, "quotes": {}}
        get_fx_up_to_date_flow.fn(date(2026, 2, 3))
    mock_insert.assert_not_called()


def test_saves_backup_file(full_quota, mock_missing_dates, run_flow):
    """Should write a backup JSON file on success."""
    mock_open = MagicMock()
    with patch("scheduled_tasks.get_fx_up_to_date.requests.get") as mock_get, \
         patch("scheduled_tasks.get_fx_up_to_date.increment_api_usage"), \
         patch("scheduled_tasks.get_fx_up_to_date.insert_fx_json"), \
         patch("builtins.open", mock_open), \
         patch("scheduled_tasks.get_fx_up_to_date.json.dump") as mock_dump:
        mock_get.return_value.json.return_value = SAMPLE_RESPONSE
        get_fx_up_to_date_flow.fn(date(2026, 2, 3))
    mock_dump.assert_called_once()


def test_aborts_when_date_range_exceeds_365_days(full_quota, run_flow):
    """Should abort when missing date range exceeds 365 day API limit, without calling the API."""
    with patch("scheduled_tasks.get_fx_up_to_date._get_missing_dates",
               return_value=["2025-01-01", "2026-06-01"]), \
         patch("scheduled_tasks.get_fx_up_to_date.requests.get") as mock_get, \
         patch("scheduled_tasks.get_fx_up_to_date.increment_api_usage") as mock_increment:
        with pytest.raises(RuntimeError, match="Date range exceeds 365 day API limit"):
            get_fx_up_to_date_flow.fn(date(2026, 6, 1))

    mock_get.assert_not_called()
    mock_increment.assert_not_called()
    run_flow.assert_not_called()


def test_target_date_defaults_to_14_days_ago(full_quota, run_flow):
    with patch("scheduled_tasks.get_fx_up_to_date._get_missing_dates", return_value=[]) as missing:
        get_fx_up_to_date_flow.fn()
    assert missing.call_args.args[0] == date.today() - timedelta(days=14)


# --- engine behaviour we no longer exercise, asserted as configuration ---

def test_api_task_keeps_its_retry_policy():
    assert fx.fetch_fx_timeframe.retries == 3
    assert fx.fetch_fx_timeframe.retry_delay_seconds == 10


def test_flow_notifies_on_failure_and_logs_on_success():
    assert fx.get_fx_up_to_date_flow.on_failure_hooks == [notify_on_completion]
    assert fx.get_fx_up_to_date_flow.on_completion_hooks == [log_on_success]
    assert fx.get_fx_up_to_date_flow.name == "Backfill FX"
