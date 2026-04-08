"""
database/exchange/table.py
~~~~~~~~~~~~~~~~~~~~~~~~~~
Schema and insert helpers for the fx_rates and api_usage tables.

FxRatesTable handles currency exchange rates fetched from the external API.
ApiUsageTable tracks monthly usage counts to avoid exceeding API limits.
Both tables are created in a single init() pass since they are closely related.
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Union

from database.base import BaseTable
from database.connection import get_conn, to_iso_str

logger = logging.getLogger(__name__)


@dataclass
class FxRateRecord:
    date: str
    source_currency: str
    target_currency: str
    rate: float
    timestamp: int | datetime | None = None


class FxRatesTable(BaseTable[FxRateRecord]):

    def init(self) -> None:
        """Create the fx_rates and api_usage tables and their indexes if they do not exist."""
        with get_conn() as conn:
            conn.execute("""
            CREATE TABLE IF NOT EXISTS fx_rates (
                id              INTEGER PRIMARY KEY,
                date            TEXT NOT NULL,
                source_currency TEXT NOT NULL,
                target_currency TEXT NOT NULL,
                rate            REAL NOT NULL,
                timestamp       TEXT NOT NULL,
                created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
                UNIQUE(date, source_currency, target_currency)
            )
            """)

            conn.execute("""
            CREATE TABLE IF NOT EXISTS api_usage (
                service     TEXT PRIMARY KEY,
                count       INTEGER NOT NULL DEFAULT 0,
                month       TEXT NOT NULL  -- YYYY-MM, to detect stale data
            )
            """)

            conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_fx_date
                ON fx_rates(date);
            """)

            conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_fx_pair
                ON fx_rates(source_currency, target_currency);
            """)

    def insert(self, record: FxRateRecord) -> None:
        """Insert a single FX rate row. Idempotent on (date, source, target)."""
        ts = to_iso_str(record.timestamp if record.timestamp is not None else datetime.now())
        logger.info(f"Inserting FX rate: {record.date} {record.source_currency}->{record.target_currency} = {record.rate}")
        with get_conn() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO fx_rates (date, source_currency, target_currency, rate, timestamp) VALUES (?, ?, ?, ?, ?)",
                (record.date, record.source_currency, record.target_currency, record.rate, ts),
            )

    def fetch_all(self, limit: int = 100) -> List[Dict]:
        """Fetch last N FX rates ordered by fetch time, newest first."""
        with get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM fx_rates ORDER BY timestamp DESC LIMIT ?",
                (limit,)
            ).fetchall()
            return [dict(row) for row in rows]

    def get_latest(
        self,
        source_currency: str,
        target_currency: str,
        return_full_row: bool = False,
    ) -> Union[float, Dict, None]:
        """Return the most recently fetched rate for a currency pair."""
        with get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM fx_rates WHERE source_currency=? AND target_currency=? ORDER BY timestamp DESC LIMIT 1",
                (source_currency, target_currency)
            ).fetchone()
            if row:
                d = dict(row)
                return d if return_full_row else d["rate"]
            return None


table = FxRatesTable()
