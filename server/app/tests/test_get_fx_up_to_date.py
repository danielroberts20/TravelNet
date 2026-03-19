# tests/test_get_fx_up_to_date.py

import pytest
from datetime import date
from unittest.mock import patch, MagicMock, call
from scheduled_tasks.get_fx_up_to_date import get_fx_up_to_date, _get_missing_dates


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


# --- _get_missing_dates ---

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
        result = _get_missing_dates(date(2026, 2, 3))
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
        result = _get_missing_dates(date(2026, 2, 3))
    assert result == []


def test_get_missing_dates_empty_db(caplog):
    """Should return empty list and log a warning when DB has no FX data."""
    mock_conn = MagicMock()
    mock_conn.__enter__ = lambda s: mock_conn
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_conn.execute.return_value.fetchall.return_value = []
    with patch("scheduled_tasks.get_fx_up_to_date.get_conn", return_value=mock_conn):
        with caplog.at_level("WARNING", logger="scheduled_tasks.get_fx_up_to_date"):
            result = _get_missing_dates(date(2026, 2, 3))
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


def test_aborts_when_no_quota_remaining(no_quota, caplog):
    """Should abort and log error when quota is exhausted."""
    with caplog.at_level("ERROR", logger="scheduled_tasks.get_fx_up_to_date"):
        get_fx_up_to_date(date(2026, 2, 3))
    assert "No API quota remaining" in caplog.text


def test_does_nothing_when_no_missing_dates(full_quota, caplog):
    """Should exit early and log info when no dates are missing."""
    with patch("scheduled_tasks.get_fx_up_to_date._get_missing_dates", return_value=[]):
        with caplog.at_level("INFO", logger="scheduled_tasks.get_fx_up_to_date"):
            get_fx_up_to_date(date(2026, 2, 3))
    assert "nothing to do" in caplog.text


def test_successful_backfill(full_quota, mock_missing_dates):
    """Should call API, increment usage, insert quotes, and save backup."""
    with patch("scheduled_tasks.get_fx_up_to_date.requests.get") as mock_get, \
         patch("scheduled_tasks.get_fx_up_to_date.increment_api_usage") as mock_increment, \
         patch("scheduled_tasks.get_fx_up_to_date.insert_fx_json") as mock_insert, \
         patch("builtins.open", MagicMock()), \
         patch("scheduled_tasks.get_fx_up_to_date.json.dump"):
        mock_get.return_value.json.return_value = SAMPLE_RESPONSE
        get_fx_up_to_date(date(2026, 2, 3))

    mock_increment.assert_called_once_with("exchangerate.host")
    mock_insert.assert_called_once_with(SAMPLE_RESPONSE["quotes"])


def test_increments_usage_even_on_api_error(full_quota, mock_missing_dates):
    """Should increment usage counter even when API returns an error."""
    with patch("scheduled_tasks.get_fx_up_to_date.requests.get") as mock_get, \
         patch("scheduled_tasks.get_fx_up_to_date.increment_api_usage") as mock_increment, \
         patch("scheduled_tasks.get_fx_up_to_date.insert_fx_json") as mock_insert:
        mock_get.return_value.json.return_value = ERROR_RESPONSE
        get_fx_up_to_date(date(2026, 2, 3))

    mock_increment.assert_called_once_with("exchangerate.host")
    mock_insert.assert_not_called()


def test_api_error_logs_error(full_quota, mock_missing_dates, caplog):
    """Should log error on API failure."""
    with patch("scheduled_tasks.get_fx_up_to_date.requests.get") as mock_get, \
         patch("scheduled_tasks.get_fx_up_to_date.increment_api_usage"), \
         caplog.at_level("ERROR", logger="scheduled_tasks.get_fx_up_to_date"):
        mock_get.return_value.json.return_value = ERROR_RESPONSE
        get_fx_up_to_date(date(2026, 2, 3))
    assert "API error" in caplog.text


def test_saves_backup_file(full_quota, mock_missing_dates):
    """Should write a backup JSON file on success."""
    mock_open = MagicMock()
    with patch("scheduled_tasks.get_fx_up_to_date.requests.get") as mock_get, \
         patch("scheduled_tasks.get_fx_up_to_date.increment_api_usage"), \
         patch("scheduled_tasks.get_fx_up_to_date.insert_fx_json"), \
         patch("builtins.open", mock_open), \
         patch("scheduled_tasks.get_fx_up_to_date.json.dump") as mock_dump:
        mock_get.return_value.json.return_value = SAMPLE_RESPONSE
        get_fx_up_to_date(date(2026, 2, 3))
    mock_dump.assert_called_once()

def test_aborts_when_date_range_exceeds_365_days(full_quota, caplog):
    """Should abort and log error when missing date range exceeds 365 day API limit."""
    with patch("scheduled_tasks.get_fx_up_to_date._get_missing_dates",
               return_value=["2025-01-01", "2026-06-01"]), \
         patch("scheduled_tasks.get_fx_up_to_date.requests.get") as mock_get, \
         patch("scheduled_tasks.get_fx_up_to_date.increment_api_usage") as mock_increment:
        with caplog.at_level("ERROR", logger="scheduled_tasks.get_fx_up_to_date"):
            get_fx_up_to_date(date(2026, 6, 1))

    assert "365 day" in caplog.text
    mock_get.assert_not_called()
    mock_increment.assert_not_called()