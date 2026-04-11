"""
test_wise_helpers.py — Unit tests for Wise parsing helpers and parse_wise_csv.
"""

import json
from unittest.mock import patch

import pytest

from database.transaction.ingest.wise import (
    _is_internal,
    _is_interest,
    _parse_timestamp,
    parse_wise_csv,
)
from database.transaction.ingest.util import safe_float as _safe_float

from conftest import make_wise_csv


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

class TestIsInternal:

    def test_conversion_with_moved_keyword(self):
        assert _is_internal("CONVERSION", "Moved 26.97 USD to 🐲 South East Asia") is True

    def test_conversion_without_internal_keyword(self):
        # A real FX exchange — not an internal pot move
        assert _is_internal("CONVERSION", "Exchanged GBP to USD") is False

    def test_money_added_with_moved_keyword(self):
        assert _is_internal("MONEY_ADDED", "Moved to pocket") is True

    def test_card_payment_not_internal(self):
        assert _is_internal("CARD", "Coffee shop") is False

    def test_transfer_to_pot(self):
        assert _is_internal("CONVERSION", "transfer to pot") is True

    def test_converted_keyword(self):
        assert _is_internal("CONVERSION", "converted 50 GBP") is True


class TestIsInterest:

    def test_accrual_checkout_detail_type(self):
        assert _is_interest("ACCRUAL_CHECKOUT", "") is True

    def test_interest_detail_type(self):
        assert _is_interest("INTEREST", "") is True

    def test_interest_keyword_in_description(self):
        assert _is_interest("CARD", "interest earned this month") is True

    def test_service_fee_keyword(self):
        assert _is_interest("CARD", "service fee applied") is True

    def test_regular_transaction_not_interest(self):
        assert _is_interest("CARD", "Supermarket") is False


class TestParseTimestamp:

    def test_standard_format(self):
        result = _parse_timestamp("05-02-2026 08:54:15.466")
        assert result == "2026-02-05T08:54:15.466000"

    def test_strips_whitespace(self):
        result = _parse_timestamp("  05-02-2026 08:54:15.466  ")
        assert result == "2026-02-05T08:54:15.466000"

    def test_invalid_format_raises(self):
        with pytest.raises(ValueError):
            _parse_timestamp("2026-02-05 08:54:15")


class TestSafeFloat:

    def test_valid_float(self):
        assert _safe_float("12.34") == pytest.approx(12.34)

    def test_empty_string_returns_none(self):
        assert _safe_float("") is None

    def test_invalid_string_returns_none(self):
        assert _safe_float("not_a_number") is None

    def test_zero(self):
        assert _safe_float("0.0") == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# parse_wise_csv
# ---------------------------------------------------------------------------

def _parse(rows, source="137103719_GBP"):
    with patch("database.transaction.ingest.wise.convert_to_gbp", return_value=-8.0):
        return parse_wise_csv(make_wise_csv(rows), source)


def test_parse_happy_path_returns_row():
    rows = _parse([{
        "TransferWise ID": "TW123",
        "Date Time": "05-02-2026 08:54:15.466",
        "Amount": "-10.00", "Currency": "GBP",
        "Description": "Coffee", "Transaction Details Type": "CARD",
        "Total fees": "0.10",
    }])
    assert len(rows) == 1
    r = rows[0]
    assert r["id"] == "TW123"
    assert r["bank"] == "Wise"
    assert r["currency"] == "GBP"
    assert r["transaction_detail"] == "CARD_PAYMENT"
    assert r["fees"] == pytest.approx(0.10)


def test_parse_filters_accrual_checkout():
    rows = _parse([
        {"TransferWise ID": "TW1", "Date Time": "05-02-2026 08:54:15.466",
         "Amount": "1.00", "Currency": "GBP", "Description": "Interest",
         "Transaction Details Type": "ACCRUAL_CHECKOUT", "Total fees": "0"},
        {"TransferWise ID": "TW2", "Date Time": "05-02-2026 09:00:00.000",
         "Amount": "-5.00", "Currency": "GBP", "Description": "Coffee",
         "Transaction Details Type": "CARD", "Total fees": "0"},
    ])
    assert len(rows) == 1
    assert rows[0]["id"] == "TW2"


def test_parse_filters_interest_detail_type():
    rows = _parse([{
        "TransferWise ID": "TW99",
        "Date Time": "05-02-2026 08:54:15.466",
        "Amount": "0.50", "Currency": "GBP", "Description": "Interest",
        "Transaction Details Type": "INTEREST", "Total fees": "0",
    }])
    assert len(rows) == 0


def test_parse_accrual_charge_flagged_as_is_interest():
    rows = _parse([{
        "TransferWise ID": "TW99",
        "Date Time": "05-02-2026 08:54:15.466",
        "Amount": "-0.50", "Currency": "GBP",
        "Description": "Asset service fee",
        "Transaction Details Type": "ACCRUAL_CHARGE", "Total fees": "0",
    }])
    assert rows[0]["is_interest"] == 1


def test_parse_internal_conversion_with_moved_keyword():
    rows = _parse([{
        "TransferWise ID": "TW50",
        "Date Time": "05-02-2026 08:54:15.466",
        "Amount": "-26.97", "Currency": "USD",
        "Description": "Moved 26.97 USD to 🐲 South East Asia",
        "Transaction Details Type": "CONVERSION", "Total fees": "0",
    }])
    assert rows[0]["is_internal"] == 1


def test_parse_real_conversion_not_flagged_internal():
    rows = _parse([{
        "TransferWise ID": "TW51",
        "Date Time": "05-02-2026 08:54:15.466",
        "Amount": "-100.00", "Currency": "GBP",
        "Description": "Exchanged GBP to USD",
        "Transaction Details Type": "CONVERSION", "Total fees": "0",
    }])
    assert rows[0]["is_internal"] == 0


def test_parse_raw_json_stored():
    rows = _parse([{
        "TransferWise ID": "TW200",
        "Date Time": "05-02-2026 08:54:15.466",
        "Amount": "-5.00", "Currency": "GBP",
        "Description": "Lunch", "Transaction Details Type": "CARD", "Total fees": "0",
    }])
    raw = json.loads(rows[0]["raw"])
    assert raw["Description"] == "Lunch"


def test_parse_empty_csv_returns_empty_list():
    assert _parse([]) == []


def test_parse_unknown_detail_type_passed_through():
    rows = _parse([{
        "TransferWise ID": "TW300",
        "Date Time": "05-02-2026 08:54:15.466",
        "Amount": "-5.00", "Currency": "GBP",
        "Description": "Mystery", "Transaction Details Type": "SOME_NEW_TYPE", "Total fees": "0",
    }])
    assert rows[0]["transaction_detail"] == "SOME_NEW_TYPE"


def test_parse_source_stored_on_row():
    rows = _parse([{
        "TransferWise ID": "TW1",
        "Date Time": "05-02-2026 08:54:15.466",
        "Amount": "-10.00", "Currency": "GBP",
        "Description": "Coffee", "Transaction Details Type": "CARD", "Total fees": "0",
    }], source="148241577_USD")
    assert rows[0]["source"] == "148241577_USD"