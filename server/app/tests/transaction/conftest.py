"""
conftest.py — Shared fixtures for transaction test suite.
"""

import io
import csv
import sqlite3
import zipfile
import sys
import pytest
from unittest.mock import patch, MagicMock

# Prevent main.py from touching the real DB on import
with patch("database.integration.init_db"), \
     patch("database.util.backup_db", return_value="/tmp/fake.db"):
    from main import app


# Prevent main.py side effects when TestClient imports it
sys.modules.setdefault("config.runtime", MagicMock())


TRANSACTIONS_DDL = """
CREATE TABLE IF NOT EXISTS transactions (
    id                  TEXT NOT NULL,
    source              TEXT NOT NULL,
    bank                TEXT NOT NULL,
    timestamp           TEXT NOT NULL,
    amount              REAL NOT NULL,
    currency            TEXT NOT NULL,
    amount_gbp          REAL,
    description         TEXT,
    payment_reference   TEXT,
    payer               TEXT,
    payee               TEXT,
    merchant            TEXT,
    fees                REAL DEFAULT 0.0,
    transaction_type    TEXT,
    transaction_detail  TEXT,
    state               TEXT,
    is_internal         INTEGER DEFAULT 0,
    is_interest         INTEGER DEFAULT 0,
    running_balance     REAL,
    raw                 TEXT NOT NULL,
    PRIMARY KEY (id, currency, source)
);
"""

REVOLUT_HEADERS = [
    "Type", "Product", "Started Date", "Completed Date",
    "Description", "Amount", "Fee", "Currency", "State", "Balance",
]

WISE_HEADERS = [
    "TransferWise ID", "Date", "Amount", "Currency", "Description",
    "Payment Reference", "Running Balance", "Exchange From", "Exchange To",
    "Exchange Rate", "Payer Name", "Payee Name", "Payee Account Number",
    "Merchant", "Total fees", "Transaction Details Type",
    "Date Time", "Transaction Type",
]


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.executescript(TRANSACTIONS_DDL)
    return conn


def row_count(db):
    return db.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]


def fetch_one(db, tx_id):
    return db.execute("SELECT * FROM transactions WHERE id = ?", (tx_id,)).fetchone()


def make_revolut_csv(rows: list[dict]) -> str:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=REVOLUT_HEADERS)
    writer.writeheader()
    for r in rows:
        writer.writerow({**{h: "" for h in REVOLUT_HEADERS}, **r})
    return buf.getvalue()


def make_wise_csv(rows: list[dict]) -> str:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=WISE_HEADERS)
    writer.writeheader()
    for r in rows:
        writer.writerow({**{h: "" for h in WISE_HEADERS}, **r})
    return buf.getvalue()


def make_wise_zip(csv_filename: str, csv_content: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(csv_filename, csv_content)
    return buf.getvalue()

