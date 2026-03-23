"""
test_upload_endpoints.py — Tests for /upload/transaction/revolut and /upload/transaction/wise.

These tests operate at the HTTP layer and mock the underlying insert functions
to isolate upload/validation logic from ingestion logic (tested separately).
"""

import io
import zipfile
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from auth import require_upload_token
from conftest import db, make_revolut_csv, make_wise_csv, make_wise_zip, app


# ---------------------------------------------------------------------------
# Revolut upload endpoint
# ---------------------------------------------------------------------------

@pytest.fixture
def revolut_client(db, tmp_path):
    # Bypass token auth for the duration of the test
    app.dependency_overrides[require_upload_token] = lambda: None
    with patch("upload.transaction.endpoints.insert_revolut") as mock_insert, \
         patch("upload.transaction.endpoints.convert_to_gbp", return_value=-8.0), \
         patch("config.general.REVOLUT_BACKUP_DIR", tmp_path):
        mock_insert.return_value = (1, 0, 0)
        with TestClient(app) as c:
            yield c, mock_insert
    app.dependency_overrides.clear()


def test_revolut_valid_csv_returns_200(revolut_client):
    c, _ = revolut_client
    csv_content = make_revolut_csv([{
        "Type": "CARD PAYMENT", "Started Date": "2026-03-01 10:00:00",
        "Amount": "-10.00", "Currency": "USD", "Description": "Starbucks",
        "State": "COMPLETED", "Fee": "0.0", "Balance": "90.00",
    }])
    resp = c.post(
        "/upload/transaction/revolut",
        files={"file": ("statement.csv", csv_content.encode(), "text/csv")},
        headers={"authorization": "Bearer testtoken"},
    )
    assert resp.status_code == 200


def test_revolut_response_is_queued(revolut_client):
    c, _ = revolut_client
    csv_content = make_revolut_csv([{
        "Type": "CARD PAYMENT", "Started Date": "2026-03-01 10:00:00",
        "Amount": "-10.00", "Currency": "USD", "Description": "Starbucks",
        "State": "COMPLETED", "Fee": "0.0", "Balance": "90.00",
    }])
    resp = c.post(
        "/upload/transaction/revolut",
        files={"file": ("statement.csv", csv_content.encode(), "text/csv")},
        headers={"authorization": "Bearer testtoken"},
    )
    assert resp.json()["status"] == "queued"


def test_revolut_non_csv_rejected(revolut_client):
    c, _ = revolut_client
    resp = c.post(
        "/upload/transaction/revolut",
        files={"file": ("statement.txt", b"data", "text/plain")},
        headers={"authorization": "Bearer testtoken"},
    )
    assert resp.status_code == 400


def test_revolut_missing_auth_rejected(tmp_path):
    # Do NOT override auth — patch settings so a token is required
    with patch("auth.settings") as mock_settings, \
         patch("config.general.REVOLUT_BACKUP_DIR", tmp_path):
        mock_settings.upload_token = "secret"
        with TestClient(app) as c:
            csv_content = make_revolut_csv([])
            resp = c.post(
                "/upload/transaction/revolut",
                files={"file": ("statement.csv", csv_content.encode(), "text/csv")},
            )
    assert resp.status_code == 401


def test_revolut_insert_called_with_csv_text(revolut_client):
    c, mock_insert = revolut_client
    csv_content = make_revolut_csv([])
    c.post(
        "/upload/transaction/revolut",
        files={"file": ("statement.csv", csv_content.encode(), "text/csv")},
        headers={"authorization": "Bearer testtoken"},
    )
    assert mock_insert.called
    called_arg = mock_insert.call_args[0][0]
    assert isinstance(called_arg, str)  # CSV text, not a file path


# ---------------------------------------------------------------------------
# Wise upload endpoint
# ---------------------------------------------------------------------------

@pytest.fixture
def wise_client(db, tmp_path):
    # Bypass token auth for the duration of the test
    app.dependency_overrides[require_upload_token] = lambda: None
    with patch("upload.transaction.endpoints.insert_wise") as mock_insert, \
         patch("config.general.WISE_BACKUP_DIR", tmp_path):
        mock_insert.return_value = (
            [{"file": "statement_137103719_GBP.csv", "inserted": 2, "parsed": 2}],
            [],
        )
        with TestClient(app) as c:
            yield c, mock_insert
    app.dependency_overrides.clear()


def _make_zip(filename="statement_137103719_GBP_20260101_20260301.csv"):
    csv_content = make_wise_csv([{
        "TransferWise ID": "TW1", "Date Time": "05-02-2026 08:54:15.466",
        "Amount": "-10.00", "Currency": "GBP", "Description": "Coffee",
        "Transaction Details Type": "CARD", "Total fees": "0",
    }])
    return make_wise_zip(filename, csv_content)


def test_wise_valid_zip_returns_200(wise_client):
    c, _ = wise_client
    resp = c.post(
        "/upload/transaction/wise",
        files={"file": ("export.zip", _make_zip(), "application/zip")},
        headers={"authorization": "Bearer testtoken"},
    )
    assert resp.status_code == 200


def test_wise_response_is_queued(wise_client):
    c, _ = wise_client
    resp = c.post(
        "/upload/transaction/wise",
        files={"file": ("export.zip", _make_zip(), "application/zip")},
        headers={"authorization": "Bearer testtoken"},
    )
    body = resp.json()
    assert body["status"] == "queued"
    assert "files" in body


def test_wise_non_zip_rejected(wise_client):
    c, _ = wise_client
    resp = c.post(
        "/upload/transaction/wise",
        files={"file": ("export.csv", b"data", "text/csv")},
        headers={"authorization": "Bearer testtoken"},
    )
    assert resp.status_code == 400


def test_wise_corrupted_zip_rejected(wise_client):
    c, _ = wise_client
    resp = c.post(
        "/upload/transaction/wise",
        files={"file": ("export.zip", b"not a zip at all", "application/zip")},
        headers={"authorization": "Bearer testtoken"},
    )
    assert resp.status_code == 400


def test_wise_zip_with_no_csv_rejected(wise_client):
    c, _ = wise_client
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("readme.txt", "no csvs here")
    resp = c.post(
        "/upload/transaction/wise",
        files={"file": ("export.zip", buf.getvalue(), "application/zip")},
        headers={"authorization": "Bearer testtoken"},
    )
    assert resp.status_code == 400


def test_wise_missing_auth_rejected(tmp_path):
    # Do NOT override auth — patch settings so a token is required
    with patch("auth.settings") as mock_settings, \
         patch("config.general.WISE_BACKUP_DIR", tmp_path):
        mock_settings.upload_token = "secret"
        with TestClient(app) as c:
            resp = c.post(
                "/upload/transaction/wise",
                files={"file": ("export.zip", _make_zip(), "application/zip")},
            )
    assert resp.status_code == 401


def test_wise_insert_called_for_each_csv_in_zip(wise_client):
    c, mock_insert = wise_client
    # Zip with two CSVs
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for fname in [
            "statement_137103719_GBP_20260101_20260301.csv",
            "statement_148241577_USD_20260101_20260301.csv",
        ]:
            zf.writestr(fname, make_wise_csv([]))
    c.post(
        "/upload/transaction/wise",
        files={"file": ("export.zip", buf.getvalue(), "application/zip")},
        headers={"authorization": "Bearer testtoken"},
    )
    assert mock_insert.call_count == 2