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
from conftest import db, make_revolut_csv, make_wise_csv, make_wise_zip, upload_log_rows, app


# ---------------------------------------------------------------------------
# Revolut upload endpoint
# ---------------------------------------------------------------------------

@pytest.fixture
def revolut_client(db, tmp_path):
    # Bypass token auth for the duration of the test
    app.dependency_overrides[require_upload_token] = lambda: None
    with patch("upload.transaction.router.insert_revolut") as mock_insert, \
         patch("upload.transaction.router.convert_to_gbp", return_value=-8.0), \
         patch("upload.transaction.router.REVOLUT_BACKUP_DIR", tmp_path), \
         patch("database.transaction.upload_log.get_conn", return_value=db):
        mock_insert.return_value = (1, 0, 0, 0)  # inserted, upgraded, skipped, errors
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
        data={"period": "2026-03"},
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
        data={"period": "2026-03"},
        headers={"authorization": "Bearer testtoken"},
    )
    assert resp.json()["status"] == "queued"


def test_revolut_non_csv_rejected(revolut_client):
    c, _ = revolut_client
    resp = c.post(
        "/upload/transaction/revolut",
        files={"file": ("statement.txt", b"data", "text/plain")},
        data={"period": "2026-03"},
        headers={"authorization": "Bearer testtoken"},
    )
    assert resp.status_code == 400


def test_revolut_missing_period_rejected(revolut_client):
    c, _ = revolut_client
    csv_content = make_revolut_csv([])
    resp = c.post(
        "/upload/transaction/revolut",
        files={"file": ("statement.csv", csv_content.encode(), "text/csv")},
        headers={"authorization": "Bearer testtoken"},
    )
    assert resp.status_code == 422


def test_revolut_malformed_period_rejected(revolut_client):
    c, _ = revolut_client
    csv_content = make_revolut_csv([])
    resp = c.post(
        "/upload/transaction/revolut",
        files={"file": ("statement.csv", csv_content.encode(), "text/csv")},
        data={"period": "March 2026"},
        headers={"authorization": "Bearer testtoken"},
    )
    assert resp.status_code == 400


def test_revolut_missing_auth_rejected(tmp_path):
    # Do NOT override auth — patch settings so a token is required
    with patch("auth.settings") as mock_settings, \
         patch("upload.transaction.router.REVOLUT_BACKUP_DIR", tmp_path):
        mock_settings.upload_token = "secret"
        with TestClient(app) as c:
            csv_content = make_revolut_csv([])
            resp = c.post(
                "/upload/transaction/revolut",
                files={"file": ("statement.csv", csv_content.encode(), "text/csv")},
                data={"period": "2026-03"},
            )
    assert resp.status_code == 401


def test_revolut_insert_called_with_csv_text(revolut_client):
    c, mock_insert = revolut_client
    csv_content = make_revolut_csv([])
    c.post(
        "/upload/transaction/revolut",
        files={"file": ("statement.csv", csv_content.encode(), "text/csv")},
        data={"period": "2026-03"},
        headers={"authorization": "Bearer testtoken"},
    )
    assert mock_insert.called
    called_arg = mock_insert.call_args[0][0]
    assert isinstance(called_arg, str)  # CSV text, not a file path


def test_revolut_upload_log_row_written(revolut_client, db):
    c, mock_insert = revolut_client
    mock_insert.return_value = (3, 1, 2, 0)  # inserted=3, upgraded=1, skipped=2
    csv_content = make_revolut_csv([])
    c.post(
        "/upload/transaction/revolut",
        files={"file": ("statement.csv", csv_content.encode(), "text/csv")},
        data={"period": "2026-03"},
        headers={"authorization": "Bearer testtoken"},
    )
    rows = upload_log_rows(db)
    assert len(rows) == 1
    assert rows[0]["source"] == "revolut"
    assert rows[0]["period_start"] == "2026-03-01"
    assert rows[0]["period_end"] == "2026-03-31"
    assert rows[0]["row_count"] == 6  # inserted + upgraded + skipped


def test_revolut_reupload_updates_existing_upload_log_row(revolut_client, db):
    c, mock_insert = revolut_client
    csv_content = make_revolut_csv([])

    mock_insert.return_value = (1, 0, 0, 0)
    c.post(
        "/upload/transaction/revolut",
        files={"file": ("statement.csv", csv_content.encode(), "text/csv")},
        data={"period": "2026-03"},
        headers={"authorization": "Bearer testtoken"},
    )
    mock_insert.return_value = (0, 0, 5, 0)
    c.post(
        "/upload/transaction/revolut",
        files={"file": ("statement.csv", csv_content.encode(), "text/csv")},
        data={"period": "2026-03"},
        headers={"authorization": "Bearer testtoken"},
    )
    rows = upload_log_rows(db)
    assert len(rows) == 1
    assert rows[0]["row_count"] == 5


# ---------------------------------------------------------------------------
# Wise upload endpoint
# ---------------------------------------------------------------------------

@pytest.fixture
def wise_client(db, tmp_path):
    # Bypass token auth for the duration of the test
    app.dependency_overrides[require_upload_token] = lambda: None
    with patch("upload.transaction.router.parse_wise_upload") as mock_upload, \
         patch("upload.transaction.router.WISE_BACKUP_DIR", tmp_path), \
         patch("database.transaction.upload_log.get_conn", return_value=db):
        mock_upload.return_value = (1, 0, 0, [])  # received, inserted, skipped, errors
        with TestClient(app) as c:
            yield c, mock_upload
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
        data={"period": "2026-01"},
        headers={"authorization": "Bearer testtoken"},
    )
    assert resp.status_code == 200


def test_wise_response_is_queued(wise_client):
    c, _ = wise_client
    resp = c.post(
        "/upload/transaction/wise",
        files={"file": ("export.zip", _make_zip(), "application/zip")},
        data={"period": "2026-01"},
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
        data={"period": "2026-01"},
        headers={"authorization": "Bearer testtoken"},
    )
    assert resp.status_code == 400


def test_wise_corrupted_zip_rejected(wise_client):
    c, _ = wise_client
    resp = c.post(
        "/upload/transaction/wise",
        files={"file": ("export.zip", b"not a zip at all", "application/zip")},
        data={"period": "2026-01"},
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
        data={"period": "2026-01"},
        headers={"authorization": "Bearer testtoken"},
    )
    assert resp.status_code == 400


def test_wise_missing_period_rejected(wise_client):
    c, _ = wise_client
    resp = c.post(
        "/upload/transaction/wise",
        files={"file": ("export.zip", _make_zip(), "application/zip")},
        headers={"authorization": "Bearer testtoken"},
    )
    assert resp.status_code == 422


def test_wise_malformed_period_rejected(wise_client):
    c, _ = wise_client
    resp = c.post(
        "/upload/transaction/wise",
        files={"file": ("export.zip", _make_zip(), "application/zip")},
        data={"period": "not-a-period"},
        headers={"authorization": "Bearer testtoken"},
    )
    assert resp.status_code == 400


def test_wise_missing_auth_rejected(tmp_path):
    # Do NOT override auth — patch settings so a token is required
    with patch("auth.settings") as mock_settings, \
         patch("upload.transaction.router.WISE_BACKUP_DIR", tmp_path):
        mock_settings.upload_token = "secret"
        with TestClient(app) as c:
            resp = c.post(
                "/upload/transaction/wise",
                files={"file": ("export.zip", _make_zip(), "application/zip")},
                data={"period": "2026-01"},
            )
    assert resp.status_code == 401


def test_wise_upload_queued_for_zip(wise_client):
    c, mock_upload = wise_client
    # Zip with two CSVs — parse_wise_upload is called once with the raw zip bytes
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
        data={"period": "2026-01"},
        headers={"authorization": "Bearer testtoken"},
    )
    assert mock_upload.call_count == 1


def test_wise_upload_log_row_written(wise_client, db):
    c, mock_upload = wise_client
    mock_upload.return_value = (2, 5, 1, [])  # received=2, inserted=5, skipped=1
    c.post(
        "/upload/transaction/wise",
        files={"file": ("export.zip", _make_zip(), "application/zip")},
        data={"period": "2026-01"},
        headers={"authorization": "Bearer testtoken"},
    )
    rows = upload_log_rows(db)
    assert len(rows) == 1
    assert rows[0]["source"] == "wise"
    assert rows[0]["period_start"] == "2026-01-01"
    assert rows[0]["period_end"] == "2026-01-31"
    assert rows[0]["row_count"] == 6  # inserted + skipped
