"""
database/transaction/table.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Schema and insert helpers for the transactions table.

All transaction sources (Revolut, Wise, Cash) share this single table.
The composite primary key (id, currency, source) allows the same transaction
ID to appear in multiple currencies without collision.

batch_insert() is provided for the ingest modules (revolut.py, wise.py) which
process CSV files and need to insert many rows in a single connection for
performance and atomicity.
"""

import logging
from dataclasses import dataclass

from database.base import BaseTable
from database.connection import get_conn

logger = logging.getLogger(__name__)


@dataclass
class TransactionRecord:
    id: str
    source: str
    bank: str
    timestamp: str          # ISO 8601 UTC — already converted by ingest caller
    amount: float
    currency: str
    raw: str                # JSON-serialised original row
    amount_gbp: float | None = None
    description: str | None = None
    payment_reference: str | None = None
    payer: str | None = None
    payee: str | None = None
    merchant: str | None = None
    fees: float = 0.0
    transaction_type: str | None = None
    transaction_detail: str | None = None
    state: str | None = None
    is_internal: int = 0
    is_interest: int = 0
    running_balance: float | None = None
    place_id: int | None = None
    category: str | None = None

TRANSACTION_TABLE_DDL = """
            CREATE TABLE IF NOT EXISTS transactions (
                id                  TEXT NOT NULL,          -- source ID or generated hash
                source              TEXT NOT NULL,          -- 'revolut', 'wise_usd', 'wise_gbp', 'cash', etc.
                bank                TEXT NOT NULL,
                timestamp           TEXT NOT NULL,          -- ISO8601, canonical event time
                amount              REAL NOT NULL,          -- original amount, negative = debit
                currency            TEXT NOT NULL,          -- ISO 4217 currency code
                amount_gbp          REAL,                   -- converted at day FX rate (NULL if unavailable)
                description         TEXT,
                payment_reference   TEXT,
                payer               TEXT,
                payee               TEXT,
                merchant            TEXT,
                fees                REAL DEFAULT 0.0,
                transaction_type    TEXT,                   -- CREDIT / DEBIT
                transaction_detail  TEXT,                   -- CARD_PAYMENT / ATM / TRANSFER etc.
                state               TEXT,                   -- COMPLETED / PENDING / FAILED (Revolut); NULL for Wise
                is_internal         INTEGER DEFAULT 0,      -- 1 = internal move between own accounts
                is_interest         INTEGER DEFAULT 0,
                running_balance     REAL,
                place_id            INTEGER REFERENCES places(id),
                raw                 TEXT NOT NULL,
                category            TEXT,
                sub_category        TEXT,
                confidence          REAL,
                PRIMARY KEY (id, currency, source)
            );
            """


class TransactionsTable(BaseTable[TransactionRecord]):

    def init(self) -> None:
        """Create the transactions table and its indexes if they do not exist."""
        with get_conn() as conn:
            conn.execute(TRANSACTION_TABLE_DDL)

            conn.execute("CREATE INDEX IF NOT EXISTS idx_txn_timestamp ON transactions(timestamp);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_txn_source ON transactions(source);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_txn_currency ON transactions(currency);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_txn_place ON transactions(place_id);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_txn_category ON transactions(category);")

    def insert(self, record: TransactionRecord) -> bool:
        """Insert a single transaction row. Returns True if inserted, False if duplicate."""
        with get_conn() as conn:
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO transactions (
                    id, source, bank, timestamp, amount, currency, amount_gbp,
                    description, payment_reference, payer, payee, merchant,
                    fees, transaction_type, transaction_detail, state,
                    is_internal, is_interest, running_balance, place_id, raw
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.id, record.source, record.bank, record.timestamp,
                    record.amount, record.currency, record.amount_gbp,
                    record.description, record.payment_reference,
                    record.payer, record.payee, record.merchant,
                    record.fees, record.transaction_type, record.transaction_detail, record.state,
                    record.is_internal, record.is_interest, record.running_balance,
                    record.place_id, record.raw,
                ),
            )
            return cursor.rowcount > 0

    def batch_insert(self, records: list[TransactionRecord]) -> tuple[int, int]:
        """Insert many transactions in a single connection.

        Used by the Revolut and Wise ingest modules to process a full CSV file
        atomically. Returns (inserted, skipped) counts.
        """
        inserted = 0
        skipped = 0
        with get_conn() as conn:
            cursor = conn.cursor()
            for record in records:
                cursor.execute(
                    """
                    INSERT OR IGNORE INTO transactions (
                        id, source, bank, timestamp, amount, currency, amount_gbp,
                        description, payment_reference, payer, payee, merchant,
                        fees, transaction_type, transaction_detail, state,
                        is_internal, is_interest, running_balance, place_id, raw
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        record.id, record.source, record.bank, record.timestamp,
                        record.amount, record.currency, record.amount_gbp,
                        record.description, record.payment_reference,
                        record.payer, record.payee, record.merchant,
                        record.fees, record.transaction_type, record.transaction_detail, record.state,
                        record.is_internal, record.is_interest, record.running_balance,
                        record.place_id, record.raw,
                    ),
                )
                if cursor.rowcount == 1:
                    inserted += 1
                else:
                    skipped += 1
            conn.commit()
        return inserted, skipped


table = TransactionsTable()
