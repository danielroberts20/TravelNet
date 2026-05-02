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

CoL normalisation columns (col_id, amount_normalised) are populated by the
monthly backfill flow, not at ingest time — they will be NULL until that
flow has run.
"""

import logging
from dataclasses import dataclass, field

from database.base import BaseTable
from database.connection import get_conn

logger = logging.getLogger(__name__)


@dataclass
class TransactionRecord:
    id: str
    source: str
    bank: str
    timestamp: str              # ISO 8601 UTC — already converted by ingest caller
    amount: float
    currency: str
    raw: str                    # JSON-serialised original row
    amount_gbp: float | None = None
    amount_normalised: float | None = None  # populated by monthly backfill
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
    col_id: int | None = None   # FK → cost_of_living(id), populated by monthly backfill
    category: str | None = None
    sub_category: str | None = None
    confidence: float | None = None


_DDL = """
    CREATE TABLE IF NOT EXISTS transactions (
        id                  TEXT NOT NULL,
        source              TEXT NOT NULL,
        bank                TEXT NOT NULL,
        timestamp           TEXT NOT NULL,
        amount              REAL NOT NULL,
        currency            TEXT NOT NULL,
        amount_gbp          REAL,
        amount_normalised   REAL,
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
        place_id            INTEGER REFERENCES places(id),
        col_id              INTEGER REFERENCES cost_of_living(id),
        category            TEXT,
        sub_category        TEXT,
        confidence          REAL,
        raw                 TEXT NOT NULL,
        PRIMARY KEY (id, currency, source)
    )
"""

_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_txn_timestamp  ON transactions(timestamp)",
    "CREATE INDEX IF NOT EXISTS idx_txn_source     ON transactions(source)",
    "CREATE INDEX IF NOT EXISTS idx_txn_currency   ON transactions(currency)",
    "CREATE INDEX IF NOT EXISTS idx_txn_place      ON transactions(place_id)",
    "CREATE INDEX IF NOT EXISTS idx_txn_col        ON transactions(col_id)",
    "CREATE INDEX IF NOT EXISTS idx_txn_category   ON transactions(category)",
]

_INSERT_SQL = """
    INSERT OR IGNORE INTO transactions (
        id, source, bank, timestamp, amount, currency,
        amount_gbp, amount_normalised,
        description, payment_reference, payer, payee, merchant,
        fees, transaction_type, transaction_detail, state,
        is_internal, is_interest, running_balance,
        place_id, col_id, category, sub_category, confidence,
        raw
    ) VALUES (
        :id, :source, :bank, :timestamp, :amount, :currency,
        :amount_gbp, :amount_normalised,
        :description, :payment_reference, :payer, :payee, :merchant,
        :fees, :transaction_type, :transaction_detail, :state,
        :is_internal, :is_interest, :running_balance,
        :place_id, :col_id, :category, :sub_category, :confidence,
        :raw
    )
"""


def _as_dict(record: TransactionRecord) -> dict:
    from dataclasses import asdict
    return asdict(record)


class TransactionsTable(BaseTable[TransactionRecord]):

    def init(self) -> None:
        with get_conn() as conn:
            conn.execute(_DDL)
            for idx in _INDEXES:
                conn.execute(idx)

    def insert(self, record: TransactionRecord) -> bool:
        """Insert a single transaction. Returns True if inserted, False if duplicate."""
        with get_conn() as conn:
            cursor = conn.execute(_INSERT_SQL, _as_dict(record))
            return cursor.rowcount > 0

    def batch_insert(self, records: list[TransactionRecord]) -> tuple[int, int]:
        """Insert many transactions in a single connection.

        Used by the Revolut and Wise ingest modules to process a full CSV file
        atomically. Returns (inserted, skipped) counts.
        """
        inserted = skipped = 0
        with get_conn() as conn:
            cursor = conn.cursor()
            for record in records:
                cursor.execute(_INSERT_SQL, _as_dict(record))
                if cursor.rowcount == 1:
                    inserted += 1
                else:
                    skipped += 1
        return inserted, skipped


table = TransactionsTable()