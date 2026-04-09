"""
test_revolut_helpers.py — Unit tests for Revolut parsing helper functions.
"""

import pytest
from database.transaction.ingest.revolut import (
    _is_internal,
    _is_interest,
    _map_detail_type,
    _generate_id,
    _parse_timestamp,
    _safe_float,
)


class TestIsInternal:

    def test_exchange_type_is_internal(self):
        assert _is_internal("EXCHANGE", "Some description") is True

    def test_to_savings_description(self):
        assert _is_internal("TRANSFER", "Moved money to savings") is True

    def test_converted_to_description(self):
        assert _is_internal("TRANSFER", "Converted to USD") is True

    def test_from_vault_description(self):
        assert _is_internal("TRANSFER", "Transferred from vault") is True

    def test_to_pocket_description(self):
        assert _is_internal("TRANSFER", "Moved to pocket") is True

    def test_regular_card_payment_not_internal(self):
        assert _is_internal("CARD PAYMENT", "Tesco groceries") is False

    def test_regular_transfer_not_internal(self):
        assert _is_internal("TRANSFER", "Sent to John") is False


class TestIsInterest:

    def test_interest_keyword(self):
        assert _is_interest("Interest payment Jan") is True

    def test_cashback_keyword(self):
        assert _is_interest("Cashback reward") is True

    def test_case_insensitive(self):
        assert _is_interest("INTEREST EARNED") is True

    def test_regular_description_not_interest(self):
        assert _is_interest("Netflix subscription") is False


class TestMapDetailType:

    def test_card_payment(self):
        assert _map_detail_type("CARD PAYMENT") == "CARD_PAYMENT"

    def test_atm(self):
        assert _map_detail_type("ATM") == "ATM"

    def test_transfer(self):
        assert _map_detail_type("TRANSFER") == "TRANSFER"

    def test_exchange(self):
        assert _map_detail_type("EXCHANGE") == "EXCHANGE"

    def test_topup(self):
        assert _map_detail_type("TOPUP") == "TOPUP"

    def test_refund(self):
        assert _map_detail_type("REFUND") == "REFUND"

    def test_unknown_type_passed_through(self):
        assert _map_detail_type("UNKNOWN_TYPE") == "UNKNOWN_TYPE"

    def test_lowercase_input_normalised(self):
        assert _map_detail_type("card payment") == "CARD_PAYMENT"


class TestGenerateId:

    def test_stable_across_calls(self):
        row = {"Started Date": "2026-03-01 10:00:00", "Amount": "-10.00",
               "Currency": "GBP", "Description": "Tesco"}
        assert _generate_id(row) == _generate_id(row)

    def test_starts_with_rev_prefix(self):
        row = {"Started Date": "2026-03-01 10:00:00", "Amount": "-10.00",
               "Currency": "GBP", "Description": "Tesco"}
        assert _generate_id(row).startswith("REV-")

    def test_differs_for_different_description(self):
        row1 = {"Started Date": "2026-03-01 10:00:00", "Amount": "-10.00",
                "Currency": "GBP", "Description": "Tesco"}
        row2 = {**row1, "Description": "Sainsburys"}
        assert _generate_id(row1) != _generate_id(row2)

    def test_differs_for_different_amount(self):
        row1 = {"Started Date": "2026-03-01 10:00:00", "Amount": "-10.00",
                "Currency": "GBP", "Description": "Tesco"}
        row2 = {**row1, "Amount": "-20.00"}
        assert _generate_id(row1) != _generate_id(row2)


class TestParseTimestamp:

    def test_standard_format(self):
        assert _parse_timestamp("2026-02-28 11:56:12") == "2026-02-28T11:56:12"

    def test_strips_whitespace(self):
        assert _parse_timestamp("  2026-02-28 11:56:12  ") == "2026-02-28T11:56:12"

    def test_invalid_format_raises(self):
        with pytest.raises(ValueError):
            _parse_timestamp("28-02-2026 11:56:12")


class TestSafeFloat:

    def test_valid_negative(self):
        assert _safe_float("-10.50") == pytest.approx(-10.50)

    def test_valid_positive(self):
        assert _safe_float("3.14") == pytest.approx(3.14)

    def test_empty_string_returns_none(self):
        assert _safe_float("") is None

    def test_invalid_string_returns_none(self):
        assert _safe_float("not_a_number") is None

    def test_zero(self):
        assert _safe_float("0") == pytest.approx(0.0)