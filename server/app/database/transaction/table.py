"""
database/transaction/table.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Schema for the transactions table.

All transaction sources (Revolut, Wise, Cash) share this single table.
The composite primary key (id, currency, source) allows the same transaction
ID to appear in multiple currencies without collision.
"""

import logging

from database.connection import get_conn

logger = logging.getLogger(__name__)


def init() -> None:
    """Create the transactions table and its indexes if they do not exist."""
    with get_conn() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id                  TEXT NOT NULL,       -- source ID or generated hash
            source              TEXT NOT NULL,          -- 'revolut', 'wise_usd', 'wise_gbp', 'cash', etc.
            bank                TEXT NOT NULL,
            timestamp           TEXT NOT NULL,          -- ISO8601, canonical event time (started_date for Revolut, datetime for Wise)
            amount              REAL NOT NULL,          -- original amount, negative = debit
            currency            TEXT NOT NULL,          -- ISO 4217 currency code
            amount_gbp          REAL,                   -- converted at day FX rate (NULL if rate unavailable)
            description         TEXT,
            payment_reference   TEXT,
            payer               TEXT,
            payee               TEXT,
            merchant            TEXT,
            fees                REAL DEFAULT 0.0,
            transaction_type    TEXT,                   -- CREDIT / DEBIT
            transaction_detail  TEXT,                   -- DEPOSIT / CONVERSION / CARD_PAYMENT / ATM / TRANSFER / ACCRUAL_CHARGE / INTEREST etc.
            state               TEXT,                   -- COMPLETED / PENDING / FAILED (Revolut); NULL for Wise
            is_internal         INTEGER DEFAULT 0,      -- 1 = pot transfer or internal move between own accounts
            is_interest         INTEGER DEFAULT 0,      -- 1 = interest payment
            running_balance     REAL,
            raw                 TEXT NOT NULL,           -- full original row as JSON blob
            
            PRIMARY KEY (id, currency, source)
        );
        """)

        # Indexes for performance
        conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_transactions_timestamp ON transactions(timestamp);
        """)
        
        conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_transactions_source    ON transactions(source);
        """)

        conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_transactions_currency  ON transactions(currency);
        """)