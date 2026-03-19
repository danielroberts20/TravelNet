"""
test_revolut_insert.py — Tests for Revolut CSV insert() logic.
"""

import json
import os
import tempfile
from unittest.mock import patch

import pytest

from conftest import db, row_count, make_revolut_csv


def run_insert(db, csv_content: str):
    """Write CSV to a temp file and call insert(), patching get_conn and convert_to_gbp."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, encoding="utf-8") as f:
        f.write(csv_content)
        path = f.name
    try:
        with patch("database.transaction.ingest.revolut.get_conn", return_value=db), \
             patch("database.transaction.ingest.revolut.convert_to_gbp", return_value=-8.0):
            from database.transaction.ingest.revolut import insert
            return insert(path)
    finally:
        os.unlink(path)


def test_happy_path_inserts_row(db):
    csv_content = make_revolut_csv([{
        "Type": "CARD PAYMENT", "Started Date": "2026-03-01 10:00:00",
        "Amount": "-10.00", "Currency": "USD", "Description": "Starbucks",
        "State": "COMPLETED", "Fee": "0.0", "Balance": "90.00",
    }])
    inserted, skipped, errors = run_insert(db, csv_content)
    assert inserted == 1
    assert skipped == 0
    assert errors == 0
    assert row_count(db) == 1


def test_row_fields_mapped_correctly(db):
    csv_content = make_revolut_csv([{
        "Type": "CARD PAYMENT", "Started Date": "2026-03-01 10:00:00",
        "Amount": "-10.00", "Currency": "USD", "Description": "Starbucks",
        "State": "COMPLETED", "Fee": "0.5", "Balance": "90.00",
    }])
    run_insert(db, csv_content)
    row = db.execute("SELECT * FROM transactions").fetchone()
    assert row["bank"] == "Revolut"
    assert row["currency"] == "USD"
    assert row["transaction_type"] == "DEBIT"
    assert row["transaction_detail"] == "CARD_PAYMENT"
    assert row["state"] == "COMPLETED"
    assert row["fees"] == pytest.approx(0.5)
    assert row["is_internal"] == 0
    assert row["is_interest"] == 0


def test_credit_transaction_type(db):
    csv_content = make_revolut_csv([{
        "Type": "TOPUP", "Started Date": "2026-03-01 10:00:00",
        "Amount": "500.00", "Currency": "GBP", "Description": "Top-up",
        "State": "COMPLETED", "Fee": "0.0", "Balance": "500.00",
    }])
    run_insert(db, csv_content)
    row = db.execute("SELECT * FROM transactions").fetchone()
    assert row["transaction_type"] == "CREDIT"


def test_deduplication_same_csv_uploaded_twice(db):
    csv_content = make_revolut_csv([{
        "Type": "CARD PAYMENT", "Started Date": "2026-03-01 10:00:00",
        "Amount": "-10.00", "Currency": "USD", "Description": "Starbucks",
        "State": "COMPLETED", "Fee": "0.0", "Balance": "90.00",
    }])
    run_insert(db, csv_content)
    inserted2, skipped2, _ = run_insert(db, csv_content)
    assert inserted2 == 0
    assert skipped2 == 1
    assert row_count(db) == 1


def test_deduplication_same_merchant_same_second(db):
    """Two rows identical in date/amount/currency/description produce same hash — second is skipped."""
    row = {
        "Type": "CARD PAYMENT", "Started Date": "2026-03-01 10:00:00",
        "Amount": "-10.00", "Currency": "USD", "Description": "Starbucks",
        "State": "COMPLETED", "Fee": "0.0", "Balance": "90.00",
    }
    csv_content = make_revolut_csv([row, row])
    inserted, skipped, errors = run_insert(db, csv_content)
    assert inserted == 1
    assert skipped == 1
    assert row_count(db) == 1


def test_skips_row_missing_started_date(db):
    csv_content = make_revolut_csv([{
        "Type": "CARD PAYMENT", "Started Date": "",
        "Amount": "-10.00", "Currency": "USD", "Description": "Starbucks",
    }])
    inserted, skipped, errors = run_insert(db, csv_content)
    assert inserted == 0
    assert skipped == 1


def test_skips_row_missing_amount(db):
    csv_content = make_revolut_csv([{
        "Type": "CARD PAYMENT", "Started Date": "2026-03-01 10:00:00",
        "Amount": "", "Currency": "USD", "Description": "Starbucks",
    }])
    inserted, skipped, errors = run_insert(db, csv_content)
    assert inserted == 0
    assert skipped == 1


def test_internal_exchange_flagged(db):
    csv_content = make_revolut_csv([{
        "Type": "EXCHANGE", "Started Date": "2026-03-01 10:00:00",
        "Amount": "-100.00", "Currency": "GBP", "Description": "Converted to USD",
        "State": "COMPLETED", "Fee": "0.0", "Balance": "400.00",
    }])
    run_insert(db, csv_content)
    row = db.execute("SELECT * FROM transactions").fetchone()
    assert row["is_internal"] == 1


def test_interest_flagged(db):
    csv_content = make_revolut_csv([{
        "Type": "REWARD", "Started Date": "2026-03-01 10:00:00",
        "Amount": "1.50", "Currency": "GBP", "Description": "Interest payment",
        "State": "COMPLETED", "Fee": "0.0", "Balance": "401.50",
    }])
    run_insert(db, csv_content)
    row = db.execute("SELECT * FROM transactions").fetchone()
    assert row["is_interest"] == 1


def test_raw_json_stored(db):
    csv_content = make_revolut_csv([{
        "Type": "CARD PAYMENT", "Started Date": "2026-03-01 10:00:00",
        "Amount": "-10.00", "Currency": "USD", "Description": "Starbucks",
        "State": "COMPLETED", "Fee": "0.0", "Balance": "90.00",
    }])
    run_insert(db, csv_content)
    row = db.execute("SELECT * FROM transactions").fetchone()
    raw = json.loads(row["raw"])
    assert raw["Description"] == "Starbucks"


def test_multiple_rows_inserted(db):
    rows = [
        {"Type": "CARD PAYMENT", "Started Date": "2026-03-01 10:00:00",
         "Amount": "-10.00", "Currency": "USD", "Description": "Starbucks",
         "State": "COMPLETED", "Fee": "0.0", "Balance": "90.00"},
        {"Type": "ATM", "Started Date": "2026-03-02 12:00:00",
         "Amount": "-50.00", "Currency": "USD", "Description": "ATM withdrawal",
         "State": "COMPLETED", "Fee": "2.0", "Balance": "40.00"},
    ]
    csv_content = make_revolut_csv(rows)
    inserted, skipped, errors = run_insert(db, csv_content)
    assert inserted == 2
    assert row_count(db) == 2


def test_empty_csv_inserts_nothing(db):
    csv_content = make_revolut_csv([])
    inserted, skipped, errors = run_insert(db, csv_content)
    assert inserted == 0
    assert row_count(db) == 0