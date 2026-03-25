"""
test_cash_endpoint.py — Tests for the /upload/transaction/cash endpoint.
"""

from datetime import datetime
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from conftest import db, row_count
from conftest import app

@pytest.fixture
def client(db):
    with patch("upload.transaction.endpoints.get_conn", return_value=db), \
         patch("upload.transaction.endpoints.convert_to_gbp", return_value=-15.0):
        with TestClient(app) as c:
            yield c


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

def test_happy_path_returns_200(client):
    resp = client.post("/upload/transaction/cash", json={
        "amount": -15.0,
        "currency": "AUD",
        "description": "Bus ticket",
    })
    assert resp.status_code == 200


def test_happy_path_response_fields(client):
    resp = client.post("/upload/transaction/cash", json={
        "amount": -15.0,
        "currency": "AUD",
        "description": "Bus ticket",
    })
    body = resp.json()
    assert body["amount"] == -15.0
    assert body["currency"] == "AUD"
    assert body["id"].startswith("CASH-")
    assert body["message"] == "Cash transaction recorded successfully."


def test_happy_path_row_inserted(client, db):
    client.post("/upload/transaction/cash", json={
        "amount": -15.0, "currency": "AUD", "description": "Bus ticket",
    })
    assert row_count(db) == 1


# ---------------------------------------------------------------------------
# Transaction type
# ---------------------------------------------------------------------------

def test_debit_transaction_type(client, db):
    client.post("/upload/transaction/cash", json={
        "amount": -20.0, "currency": "USD", "description": "Taxi",
    })
    row = db.execute("SELECT * FROM transactions").fetchone()
    assert row["transaction_type"] == "DEBIT"


def test_credit_transaction_type(client, db):
    client.post("/upload/transaction/cash", json={
        "amount": 50.0, "currency": "USD", "description": "Received cash",
    })
    row = db.execute("SELECT * FROM transactions").fetchone()
    assert row["transaction_type"] == "CREDIT"


# ---------------------------------------------------------------------------
# Timestamp handling
# ---------------------------------------------------------------------------

def test_custom_timestamp_used(client, db):
    client.post("/upload/transaction/cash", json={
        "amount": -5.0, "currency": "GBP", "description": "Coffee",
        "timestamp": "2026-09-14T14:30:00",
    })
    row = db.execute("SELECT * FROM transactions").fetchone()
    assert row["timestamp"] == "2026-09-14T14:30:00Z"


def test_defaults_to_now_when_no_timestamp(client, db):
    client.post("/upload/transaction/cash", json={
        "amount": -5.0, "currency": "GBP", "description": "Coffee",
    })
    row = db.execute("SELECT * FROM transactions").fetchone()
    # Just verify it's a parseable ISO datetime
    datetime.fromisoformat(row["timestamp"])


# ---------------------------------------------------------------------------
# Currency normalisation
# ---------------------------------------------------------------------------

def test_currency_uppercased(client):
    resp = client.post("/upload/transaction/cash", json={
        "amount": -5.0, "currency": "aud", "description": "Coffee",
    })
    assert resp.status_code == 200
    assert resp.json()["currency"] == "AUD"


# ---------------------------------------------------------------------------
# DB field values
# ---------------------------------------------------------------------------

def test_source_set_to_cash(client, db):
    client.post("/upload/transaction/cash", json={
        "amount": -5.0, "currency": "GBP", "description": "Coffee",
    })
    row = db.execute("SELECT * FROM transactions").fetchone()
    assert row["source"] == "cash"


def test_bank_set_to_cash(client, db):
    client.post("/upload/transaction/cash", json={
        "amount": -5.0, "currency": "GBP", "description": "Coffee",
    })
    row = db.execute("SELECT * FROM transactions").fetchone()
    assert row["bank"] == "Cash"


def test_state_set_to_completed(client, db):
    client.post("/upload/transaction/cash", json={
        "amount": -5.0, "currency": "GBP", "description": "Coffee",
    })
    row = db.execute("SELECT * FROM transactions").fetchone()
    assert row["state"] == "COMPLETED"


def test_is_internal_and_is_interest_are_zero(client, db):
    client.post("/upload/transaction/cash", json={
        "amount": -5.0, "currency": "GBP", "description": "Coffee",
    })
    row = db.execute("SELECT * FROM transactions").fetchone()
    assert row["is_internal"] == 0
    assert row["is_interest"] == 0


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

def test_duplicate_post_silently_ignored(client, db):
    payload = {
        "amount": -5.0, "currency": "GBP", "description": "Coffee",
        "timestamp": "2026-09-14T14:30:00",
    }
    r1 = client.post("/upload/transaction/cash", json=payload)
    r2 = client.post("/upload/transaction/cash", json=payload)
    assert r1.status_code == 200
    assert r2.status_code == 200
    assert r1.json()["id"] == r2.json()["id"]
    assert row_count(db) == 1


# ---------------------------------------------------------------------------
# Validation errors
# ---------------------------------------------------------------------------

def test_invalid_timestamp_rejected(client):
    resp = client.post("/upload/transaction/cash", json={
        "amount": -5.0, "currency": "GBP", "description": "Coffee",
        "timestamp": "not-a-date",
    })
    assert resp.status_code == 422


def test_missing_description_rejected(client):
    resp = client.post("/upload/transaction/cash", json={
        "amount": -5.0, "currency": "GBP",
    })
    assert resp.status_code == 422


def test_currency_too_short_rejected(client):
    resp = client.post("/upload/transaction/cash", json={
        "amount": -5.0, "currency": "GB", "description": "Coffee",
    })
    assert resp.status_code == 422


def test_currency_too_long_rejected(client):
    resp = client.post("/upload/transaction/cash", json={
        "amount": -5.0, "currency": "GBPP", "description": "Coffee",
    })
    assert resp.status_code == 422


def test_missing_amount_rejected(client):
    resp = client.post("/upload/transaction/cash", json={
        "currency": "GBP", "description": "Coffee",
    })
    assert resp.status_code == 422