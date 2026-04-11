"""
test_wise_insert.py — Tests for Wise insert() logic.
"""

import io
import zipfile
from unittest.mock import patch

import pytest

from conftest import db, row_count, make_wise_csv, make_wise_zip


def make_zf(csv_rows: list[dict], filename: str = "statement_137103719_GBP.csv"):
    csv_content = make_wise_csv(csv_rows)
    zip_bytes = make_wise_zip(filename, csv_content)
    return zipfile.ZipFile(io.BytesIO(zip_bytes)), filename


def run_insert(db, csv_rows, filename="statement_137103719_GBP.csv", source="137103719_GBP"):
    zf, fname = make_zf(csv_rows, filename)
    with patch("database.transaction.ingest.wise.get_conn", return_value=db), \
         patch("database.transaction.ingest.wise.convert_to_gbp", return_value=-8.0), \
         patch("database.transaction.ingest.wise.get_closest_lat_lon_by_timestamp", return_value=(None, None)):
        from database.transaction.ingest.wise import insert
        return insert(zf, fname, source)


def test_happy_path_inserts_row(db):
    results, errors = run_insert(db, [{
        "TransferWise ID": "TW1",
        "Date Time": "05-02-2026 08:54:15.466",
        "Amount": "-10.00", "Currency": "GBP",
        "Description": "Coffee", "Transaction Details Type": "CARD", "Total fees": "0.10",
    }])
    assert errors == []
    assert results[0]["inserted"] == 1
    assert row_count(db) == 1


def test_deduplication_same_row_twice(db):
    row = {
        "TransferWise ID": "TW1",
        "Date Time": "05-02-2026 08:54:15.466",
        "Amount": "-10.00", "Currency": "GBP",
        "Description": "Coffee", "Transaction Details Type": "CARD", "Total fees": "0",
    }
    run_insert(db, [row])
    results, errors = run_insert(db, [row])
    assert errors == []
    assert results[0]["inserted"] == 0
    assert row_count(db) == 1


def test_empty_csv_returns_result_with_zero_inserted(db):
    results, errors = run_insert(db, [])
    assert errors == []
    assert row_count(db) == 0


def test_interest_rows_filtered_before_insert(db):
    """ACCRUAL_CHECKOUT rows are dropped in parse_wise_csv and never reach the DB."""
    results, errors = run_insert(db, [{
        "TransferWise ID": "TW99",
        "Date Time": "05-02-2026 08:54:15.466",
        "Amount": "1.00", "Currency": "GBP",
        "Description": "Interest", "Transaction Details Type": "ACCRUAL_CHECKOUT", "Total fees": "0",
    }])
    assert row_count(db) == 0


def test_multiple_rows_inserted(db):
    rows = [
        {"TransferWise ID": "TW1", "Date Time": "05-02-2026 08:54:15.466",
         "Amount": "-10.00", "Currency": "GBP", "Description": "Coffee",
         "Transaction Details Type": "CARD", "Total fees": "0"},
        {"TransferWise ID": "TW2", "Date Time": "06-02-2026 10:00:00.000",
         "Amount": "-50.00", "Currency": "GBP", "Description": "Supermarket",
         "Transaction Details Type": "CARD", "Total fees": "0"},
    ]
    results, errors = run_insert(db, rows)
    assert results[0]["inserted"] == 2
    assert row_count(db) == 2


def test_source_stored_correctly(db):
    run_insert(db, [{
        "TransferWise ID": "TW1",
        "Date Time": "05-02-2026 08:54:15.466",
        "Amount": "-10.00", "Currency": "GBP",
        "Description": "Coffee", "Transaction Details Type": "CARD", "Total fees": "0",
    }], source="137103719_GBP")
    row = db.execute("SELECT source FROM transactions").fetchone()
    assert row["source"] == "137103719_GBP"


def test_parsed_count_matches_inserted(db):
    rows = [
        {"TransferWise ID": "TW1", "Date Time": "05-02-2026 08:54:15.466",
         "Amount": "-10.00", "Currency": "GBP", "Description": "Coffee",
         "Transaction Details Type": "CARD", "Total fees": "0"},
        {"TransferWise ID": "TW2", "Date Time": "06-02-2026 10:00:00.000",
         "Amount": "-5.00", "Currency": "GBP", "Description": "Tea",
         "Transaction Details Type": "CARD", "Total fees": "0"},
    ]
    results, _ = run_insert(db, rows)
    assert results[0]["parsed"] == results[0]["inserted"] == 2


def test_returns_error_entry_for_bad_zip_file(db):
    """Passing a ZipFile with a filename that doesn't exist inside it should surface an error."""
    csv_content = make_wise_csv([])
    zip_bytes = make_wise_zip("statement_137103719_GBP.csv", csv_content)
    zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    with patch("database.transaction.ingest.wise.get_conn", return_value=db), \
         patch("database.transaction.ingest.wise.convert_to_gbp", return_value=-8.0):
        from database.transaction.ingest.wise import insert
        results, errors = insert(zf, "nonexistent_file.csv", "137103719_GBP")
    assert len(errors) == 1
    assert "nonexistent_file.csv" in errors[0]["file"]