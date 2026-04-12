import pytest
from unittest.mock import patch, MagicMock, call
from datetime import datetime
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