"""
test_commbank_insert.py — Tests for CommBank insert() logic and the upload endpoint.
"""

import json
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from auth import require_upload_token
from conftest import app, db, row_count, upload_log_rows
from database.transaction.ingest.commbank import CommbankParseError, _assign_ids, parse_commbank_text
from test_commbank_parse import SAMPLE, make_text


def parsed(text=SAMPLE):
    txs = parse_commbank_text(text)
    _assign_ids(txs)
    return txs


def run_insert(db, txs):
    with patch("database.transaction.ingest.commbank.get_conn", return_value=db), \
         patch("database.transaction.ingest.commbank.convert_to_gbp", return_value=-4.0), \
         patch("database.transaction.ingest.commbank.get_nearest_lat_lon_within_hours", return_value=(None, None)), \
         patch("database.transaction.ingest.commbank.get_place_id", return_value=1), \
         patch("database.transaction.ingest.commbank.get_col_entry", return_value=None):
        from database.transaction.ingest.commbank import insert
        return insert(txs)


def test_inserts_all_rows(db):
    assert run_insert(db, parsed()) == (4, 0, 0)
    assert row_count(db) == 4


def test_reupload_is_idempotent(db):
    run_insert(db, parsed())
    assert run_insert(db, parsed()) == (0, 4, 0)
    assert row_count(db) == 4


def test_card_row_uses_value_date_for_timestamp(db):
    run_insert(db, parsed())
    row = db.execute("SELECT * FROM transactions WHERE description LIKE 'KFC%'").fetchone()
    assert row["timestamp"] == "2026-09-13T12:00:00Z"
    raw = json.loads(row["raw"])
    assert raw["posted_date"] == "2026-09-15"
    assert raw["value_date"] == "2026-09-13"
    assert raw["card_suffix"] == "0488"


def test_row_without_value_date_uses_posted_date(db):
    run_insert(db, parsed())
    row = db.execute("SELECT * FROM transactions WHERE description LIKE 'SOME CAFE%'").fetchone()
    assert row["timestamp"] == "2026-09-13T12:00:00Z"


def test_fields_mapped(db):
    run_insert(db, parsed())
    row = db.execute("SELECT * FROM transactions WHERE description LIKE 'KFC%'").fetchone()
    assert row["bank"] == "CommBank"
    assert row["source"] == "commbank"
    assert row["currency"] == "AUD"
    assert row["amount"] == pytest.approx(-12.95)
    assert row["running_balance"] == pytest.approx(79.93)
    assert row["transaction_type"] == "DEBIT"
    assert row["transaction_detail"] == "CARD_PAYMENT"
    assert row["state"] == "COMPLETED"
    assert row["is_internal"] == 0


def test_self_transfers_marked_internal(db):
    # Names in config.general.SELF_NAMES
    text = make_text(
        "11 Sep 2026 Fast Transfer From Daniel James Roberts $50.00 $50.00\n"
        "12 Sep 2026 Transfer To Daniel James Roberts -$10.00 $40.00\n"
    )
    run_insert(db, parsed(text))
    rows = db.execute("SELECT * FROM transactions ORDER BY timestamp").fetchall()
    assert [r["is_internal"] for r in rows] == [1, 1]
    assert [r["transaction_detail"] for r in rows] == ["TRANSFER", "TRANSFER"]
    assert rows[0]["payer"] == "Daniel James Roberts"
    assert rows[1]["payee"] == "Daniel James Roberts"


def test_atm_detail(db):
    text = make_text("14 Sep 2026 Wdl ATM CBA ATM ST KILDA B VIC 316902 AUS -$20.00 $80.00\n")
    run_insert(db, parsed(text))
    assert db.execute("SELECT transaction_detail FROM transactions").fetchone()[0] == "ATM"


def test_bad_row_counted_as_error_not_fatal(db):
    txs = parsed()
    del txs[1]["posted_date"]
    inserted, skipped, errors = run_insert(db, txs)
    assert (inserted, errors) == (3, 1)


# ---------------------------------------------------------------------------
# Upload endpoint
# ---------------------------------------------------------------------------

@pytest.fixture
def commbank_client(db, tmp_path):
    app.dependency_overrides[require_upload_token] = lambda: None
    with patch("upload.transaction.router.parse_commbank_pdf") as mock_parse, \
         patch("upload.transaction.router.insert_commbank") as mock_insert, \
         patch("upload.transaction.router.COMMBANK_BACKUP_DIR", tmp_path), \
         patch("database.transaction.upload_log.get_conn", return_value=db):
        mock_parse.return_value = [{"id": "x"}]
        mock_insert.return_value = (1, 0, 0)  # inserted, skipped, errors
        with TestClient(app) as c:
            yield c, mock_parse, mock_insert, tmp_path
    app.dependency_overrides.clear()


def _post(c, name="TransactionSummary.pdf", period="2026-09"):
    return c.post(
        "/upload/transaction/commbank",
        files={"file": (name, b"%PDF-fake", "application/pdf")},
        data={"period": period},
        headers={"authorization": "Bearer testtoken"},
    )


def test_endpoint_queues_and_backs_up(commbank_client):
    c, _, mock_insert, tmp_path = commbank_client
    resp = _post(c)
    assert resp.status_code == 200
    assert resp.json()["status"] == "queued"
    assert len(list(tmp_path.glob("*.pdf"))) == 1
    mock_insert.assert_called_once()


def test_endpoint_records_upload_log(commbank_client, db):
    c, *_ = commbank_client
    _post(c)
    rows = upload_log_rows(db)
    assert len(rows) == 1
    assert rows[0]["source"] == "commbank"
    assert rows[0]["period_start"] == "2026-09-01"
    assert rows[0]["period_end"] == "2026-09-30"
    assert rows[0]["row_count"] == 1


def test_endpoint_rejects_non_pdf(commbank_client):
    c, mock_parse, *_ = commbank_client
    assert _post(c, name="statement.csv").status_code == 400
    mock_parse.assert_not_called()


def test_endpoint_rejects_bad_period(commbank_client):
    c, *_ = commbank_client
    assert _post(c, period="September").status_code == 400


def test_endpoint_rejects_unparseable_pdf(commbank_client):
    c, mock_parse, mock_insert, tmp_path = commbank_client
    mock_parse.side_effect = CommbankParseError("No transactions found")
    resp = _post(c)
    assert resp.status_code == 400
    assert "No transactions" in resp.json()["detail"]
    assert list(tmp_path.glob("*.pdf")) == []
    mock_insert.assert_not_called()
