import pytest
from unittest.mock import patch, MagicMock, call
from datetime import datetime
from scheduled_tasks.get_fx import get_fx_rate_at_date, get_fx_for_month
from database.exchange.fx import insert_fx_json


# --- Sample data ---

SAMPLE_TIMEFRAME_RESPONSE = {
    "success": True,
    "timeframe": True,
    "start_date": "2026-02-01",
    "end_date": "2026-02-28",
    "source": "GBP",
    "quotes": {
        "2026-02-01": {"GBPUSD": 1.367811, "GBPAUD": 1.969313},
        "2026-02-02": {"GBPUSD": 1.367238, "GBPAUD": 1.965793},
    }
}

SAMPLE_SINGLE_DATE_RESPONSE = {
    "success": True,
    "date": "2026-02-01",
    "source": "GBP",
    "quotes": {"GBPUSD": 1.367811, "GBPAUD": 1.969313},
}

SAMPLE_RATE_LIMIT_RESPONSE = {
    "success": False,
    "error": {"type": "rate_limit_reached", "info": "Rate limit reached"}
}

SAMPLE_ERROR_RESPONSE = {
    "success": False,
    "error": {"type": "invalid_access_key", "info": "Invalid key"}
}

@pytest.fixture(autouse=True)
def mock_increment_api_usage():
    """Prevent all tests from hitting the api_usage table."""
    with patch("scheduled_tasks.get_fx.increment_api_usage"):
        yield

# --- get_fx_rate_at_date ---

def test_get_fx_rate_at_date_success():
    with patch("scheduled_tasks.get_fx.requests.get") as mock_get:
        mock_get.return_value.json.return_value = SAMPLE_SINGLE_DATE_RESPONSE
        result = get_fx_rate_at_date("2026-02-01")
    assert result["success"] is True
    assert "GBPUSD" in result["quotes"]


def test_get_fx_rate_at_date_invalid_date():
    result = get_fx_rate_at_date("not-a-date")
    assert result is None


def test_get_fx_rate_at_date_api_error():
    with patch("scheduled_tasks.get_fx.requests.get") as mock_get:
        mock_get.return_value.json.return_value = SAMPLE_ERROR_RESPONSE
        result = get_fx_rate_at_date("2026-02-01")
    assert result is None


def test_get_fx_rate_at_date_retries_on_rate_limit():
    """Should retry once on rate limit then succeed."""
    rate_limit_mock = MagicMock()
    rate_limit_mock.json.return_value = SAMPLE_RATE_LIMIT_RESPONSE

    success_mock = MagicMock()
    success_mock.json.return_value = SAMPLE_SINGLE_DATE_RESPONSE

    with patch("scheduled_tasks.get_fx.requests.get") as mock_get, \
         patch("scheduled_tasks.get_fx.time.sleep") as mock_sleep:
        mock_get.side_effect = [rate_limit_mock, success_mock]
        result = get_fx_rate_at_date("2026-02-01", 5)

    mock_sleep.assert_called_once_with(5)
    assert result["success"] is True


# --- get_fx_for_month ---

def test_get_fx_for_month_makes_single_api_call():
    """Should make exactly one API call for the entire month."""
    with patch("scheduled_tasks.get_fx.requests.get") as mock_get, \
         patch("scheduled_tasks.get_fx.insert_fx_json") as mock_insert, \
         patch("builtins.open", MagicMock()), \
         patch("scheduled_tasks.get_fx.json.dump"):
        mock_get.return_value.json.return_value = SAMPLE_TIMEFRAME_RESPONSE
        get_fx_for_month(month=2, year=2026)
    assert mock_get.call_count == 1


def test_get_fx_for_month_calls_insert_with_quotes():
    """Should pass the quotes dict to insert_fx_json."""
    with patch("scheduled_tasks.get_fx.requests.get") as mock_get, \
         patch("scheduled_tasks.get_fx.insert_fx_json") as mock_insert, \
         patch("builtins.open", MagicMock()), \
         patch("scheduled_tasks.get_fx.json.dump"):
        mock_get.return_value.json.return_value = SAMPLE_TIMEFRAME_RESPONSE
        get_fx_for_month(month=2, year=2026)
    mock_insert.assert_called_once_with(SAMPLE_TIMEFRAME_RESPONSE["quotes"])


def test_get_fx_for_month_api_failure_logs_error(caplog):
    """Should log an error and raise RuntimeError on API failure."""
    with patch("scheduled_tasks.get_fx.requests.get") as mock_get, \
         patch("scheduled_tasks.get_fx.insert_fx_json") as mock_insert:
        mock_get.return_value.json.return_value = SAMPLE_ERROR_RESPONSE
        with caplog.at_level("ERROR", logger="scheduled_tasks.get_fx"):
            with pytest.raises(RuntimeError):
                get_fx_for_month(month=2, year=2026)
    mock_insert.assert_not_called()
    assert "FX API error" in caplog.text


def test_get_fx_for_month_clamps_month():
    """Month should be clamped between 1 and 12."""
    with patch("scheduled_tasks.get_fx.requests.get") as mock_get, \
         patch("scheduled_tasks.get_fx.insert_fx_json"), \
         patch("builtins.open", MagicMock()), \
         patch("scheduled_tasks.get_fx.json.dump"):
        mock_get.return_value.json.return_value = SAMPLE_TIMEFRAME_RESPONSE
        get_fx_for_month(month=99, year=2026)
    _, kwargs = mock_get.call_args
    assert "end_date" in kwargs["params"]
    assert kwargs["params"]["end_date"].startswith("2026-12")


def test_get_fx_for_month_saves_backup():
    """Should write a backup JSON file."""
    mock_open = MagicMock()
    with patch("scheduled_tasks.get_fx.requests.get") as mock_get, \
         patch("scheduled_tasks.get_fx.insert_fx_json"), \
         patch("builtins.open", mock_open), \
         patch("scheduled_tasks.get_fx.json.dump") as mock_dump:
        mock_get.return_value.json.return_value = SAMPLE_TIMEFRAME_RESPONSE
        get_fx_for_month(month=2, year=2026)
    mock_dump.assert_called_once()


# --- insert_fx_json ---

def test_insert_fx_json_inserts_all_dates():
    """Should call fx_table.insert once per date per currency pair."""
    quotes = {
        "2026-02-01": {"GBPUSD": 1.36, "GBPAUD": 1.97},
        "2026-02-02": {"GBPUSD": 1.37, "GBPAUD": 1.96},
    }
    with patch("database.exchange.fx.fx_table") as mock_table:
        insert_fx_json(quotes)
    assert mock_table.insert.call_count == 4  # 2 dates x 2 currencies


def test_insert_fx_json_skips_wrong_source(caplog):
    """Should skip and warn if source currency doesn't match SOURCE_CURRENCY."""
    quotes = {
        "2026-02-01": {"USDGBP": 0.73, "GBPUSD": 1.36},
    }
    with patch("database.exchange.fx.fx_table") as mock_table, \
         patch("database.exchange.fx.SOURCE_CURRENCY", "GBP"):
        with caplog.at_level("WARNING", logger="database.exchange.fx"):
            insert_fx_json(quotes)
    assert mock_table.insert.call_count == 1  # only GBPUSD inserted
    assert "Unexpected source currency" in caplog.text


def test_insert_fx_rate_uses_correct_values():
    """Should insert with correct date, currencies, and rate."""
    from database.exchange.table import FxRateRecord
    quotes = {"2026-02-01": {"GBPUSD": 1.367811}}
    with patch("database.exchange.fx.fx_table") as mock_table, \
         patch("database.exchange.fx.SOURCE_CURRENCY", "GBP"):
        insert_fx_json(quotes)
    mock_table.insert.assert_called_once_with(FxRateRecord(
        date="2026-02-01",
        source_currency="GBP",
        target_currency="USD",
        rate=1.367811,
        timestamp=pytest.approx(int(datetime.strptime("2026-02-01", "%Y-%m-%d").timestamp()), abs=1),
    ))