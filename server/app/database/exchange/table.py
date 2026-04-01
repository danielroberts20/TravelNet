"""
database/exchange/table.py
~~~~~~~~~~~~~~~~~~~~~~~~~~
Schema and insert helpers for FX rate and API-usage tables.
"""

from datetime import datetime
from typing import Dict, List, Optional, Union
import logging

from database.connection import get_conn, to_iso_str

logger = logging.getLogger(__name__)


def init() -> None:
    """Create the fx_rates and api_usage tables and their indexes if they do not exist."""
    with get_conn() as conn:
        # FX rates table
        conn.execute("""
        CREATE TABLE IF NOT EXISTS fx_rates (
            id INTEGER PRIMARY KEY,
            date TEXT NOT NULL,
            source_currency TEXT NOT NULL,
            target_currency TEXT NOT NULL,
            rate REAL NOT NULL,
            timestamp TEXT NOT NULL,            -- Time when fetched
            created_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),

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

        # Indexes for performance
        conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_fx_date
            ON fx_rates(date);
        """)

        conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_fx_source_target
            ON fx_rates(source_currency, target_currency);
        """)


# -----------------------------
# FX helpers
# -----------------------------

def insert_fx_rate(
    date: str,
    source_currency: str,
    target_currency: str,
    rate: float,
    ts: Optional[int] = None,
) -> int:
    """Insert a single FX rate row. Returns the lastrowid (0 if already existed).

    ts defaults to the current Unix timestamp if not provided.
    Uses INSERT OR IGNORE so duplicate (date, source, target) rows are silently skipped.
    """
    if ts is None:
        ts = datetime.now()
    new_ts = to_iso_str(ts)
    new_date = date
    logger.info(f"Inserting FX rate: {date} {source_currency}->{target_currency} = {rate}")
    with get_conn() as conn:
        cursor = conn.execute(
            "INSERT OR IGNORE INTO fx_rates (date, source_currency, target_currency, rate, timestamp) VALUES (?, ?, ?, ?, ?)",
            (new_date, source_currency, target_currency, rate, new_ts)
        )
        logger.info(f"Inserted FX rate with ID {cursor.lastrowid}")
        return cursor.lastrowid


def fetch_fx_rates(limit: int = 100) -> List[Dict]:
    """Fetch last N FX rates ordered by fetch time, newest first."""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM fx_rates ORDER BY timestamp DESC LIMIT ?",
            (limit,)
        ).fetchall()
        return [dict(row) for row in rows]


def get_latest_fx(
    source_currency: str,
    target_currency: str,
    return_full_row: bool = False,
) -> Union[float, Dict, None]:
    """Return the most recently fetched rate for a currency pair.

    Parameters
    ----------
    source_currency:  e.g. 'GBP'
    target_currency:  e.g. 'USD'
    return_full_row:  When True return the full row dict; when False return
                      just the rate float.  Returns None if no rate exists.
    """
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM fx_rates WHERE source_currency=? AND target_currency=? ORDER BY timestamp DESC LIMIT 1",
            (source_currency, target_currency)
        ).fetchone()
        if row:
            d = dict(row)
            if return_full_row:
                return d
            return d["rate"]
        return None
