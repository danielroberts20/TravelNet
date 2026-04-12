import logging
import sqlite3
import pytest
from unittest.mock import patch, MagicMock
from scheduled_tasks.backfill_gbp import backfill_gbp_flow


@pytest.fixture
def db():
    """In-memory SQLite DB with transactions and fx_rates tables."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE transactions (
            id                TEXT NOT NULL,
            source            TEXT NOT NULL,
            bank              TEXT NOT NULL,
            timestamp         TEXT NOT NULL,
            amount            REAL NOT NULL,
            currency          TEXT NOT NULL,
            amount_gbp        REAL,
            description       TEXT,
            payment_reference TEXT,
            payer             TEXT,
            payee             TEXT,
            merchant          TEXT,
            fees              REAL DEFAULT 0.0,
            transaction_type  TEXT,
            transaction_detail TEXT,
            state             TEXT,
            is_internal       INTEGER DEFAULT 0,
            is_interest       INTEGER DEFAULT 0,
            running_balance   REAL,
            raw               TEXT NOT NULL,
            PRIMARY KEY (id, currency, source)
        );

        CREATE TABLE fx_rates (
            id              INTEGER PRIMARY KEY,
            date            TEXT NOT NULL,
            source_currency TEXT NOT NULL,
            target_currency TEXT NOT NULL,
            rate            REAL NOT NULL,
            timestamp       TEXT NOT NULL,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(date, source_currency, target_currency)
        );
    """)
    return conn


def make_transaction(id, currency, amount, timestamp="2026-03-01T12:00:00", amount_gbp=None):
    return (id, "revolut", "Revolut", timestamp, amount, currency, amount_gbp, None, None, None, None, None, 0.0, "DEBIT", "CARD_PAYMENT", "COMPLETED", 0, 0, None, "{}")


def test_backfills_foreign_currency(db):
    """Transaction in USD with available FX rate gets amount_gbp set."""
    db.execute("INSERT INTO transactions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", make_transaction("tx1", "USD", -50.0))
    db.execute("INSERT INTO fx_rates (date, source_currency, target_currency, rate, timestamp) VALUES (?,?,?,?,?)",
               ("2026-03-01", "GBP", "USD", 1.25, 0))
    db.commit()

    with patch("scheduled_tasks.backfill_gbp.get_conn", return_value=db):
        backfill_gbp_flow()

    row = db.execute("SELECT amount_gbp FROM transactions WHERE id = 'tx1'").fetchone()
    assert row["amount_gbp"] == pytest.approx(-40.0, rel=1e-4)  # -50 / 1.25


def test_backfills_gbp_transaction(db):
    """GBP transaction gets amount_gbp = amount without needing FX lookup."""
    db.execute("INSERT INTO transactions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", make_transaction("tx2", "GBP", -20.0))
    db.commit()

    with patch("scheduled_tasks.backfill_gbp.get_conn", return_value=db):
        backfill_gbp_flow()

    row = db.execute("SELECT amount_gbp FROM transactions WHERE id = 'tx2'").fetchone()
    assert row["amount_gbp"] == pytest.approx(-20.0)


def test_logs_warning_when_fx_missing(db, caplog):
    """Transaction with no FX rate available logs a warning and stays NULL."""
    db.execute("INSERT INTO transactions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", make_transaction("tx3", "AUD", -100.0))
    db.commit()

    with patch("scheduled_tasks.backfill_gbp.get_conn", return_value=db):
        with caplog.at_level(logging.WARNING, logger="scheduled_tasks.backfill_gbp"):
            backfill_gbp_flow()

    row = db.execute("SELECT amount_gbp FROM transactions WHERE id = 'tx3'").fetchone()
    assert row["amount_gbp"] is None
    assert "still NULL" in caplog.text
    assert "tx3" in caplog.text


def test_no_nulls_does_nothing(db, caplog):
    """No NULL transactions — function exits early and logs info."""
    db.execute("INSERT INTO transactions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", make_transaction("tx4", "GBP", -10.0, amount_gbp=-10.0))
    db.commit()

    with patch("scheduled_tasks.backfill_gbp.get_conn", return_value=db):
        with caplog.at_level(logging.INFO, logger="scheduled_tasks.backfill_gbp"):
            backfill_gbp_flow()

    assert "No NULL amount_gbp transactions found." in caplog.text


def test_does_not_overwrite_existing_amount_gbp(db):
    """Transactions already having amount_gbp set are not touched."""
    db.execute("INSERT INTO transactions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", make_transaction("tx5", "USD", -50.0, amount_gbp=-99.0))
    db.commit()

    with patch("scheduled_tasks.backfill_gbp.get_conn", return_value=db):
        backfill_gbp_flow()

    row = db.execute("SELECT amount_gbp FROM transactions WHERE id = 'tx5'").fetchone()
    assert row["amount_gbp"] == pytest.approx(-99.0)  # unchanged