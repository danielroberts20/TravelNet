from datetime import datetime
from typing import Dict, List, Optional

from database.util import get_conn

def init():
    with get_conn() as conn:
        # FX rates table (optional)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS fx_rates (
            id INTEGER PRIMARY KEY,
            date TEXT NOT NULL,                -- YYYY-MM-DD
            source_currency TEXT NOT NULL,
            target_currency TEXT NOT NULL,
            rate REAL NOT NULL,
            timestamp INTEGER NOT NULL,        -- Time when fetched
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        
            UNIQUE(date, source_currency, target_currency)
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
def insert_fx_rate(date: str, source_currency: str, target_currency: str, rate: float, ts: Optional[int] = None) -> int:
    """Insert FX rate. ts defaults to now if not provided."""
    if ts is None:
        ts = int(datetime.now().timestamp())
    with get_conn() as conn:
        cursor = conn.execute(
            "INSERT INTO fx_rates (date, source_currency, target_currency, rate, timestamp) VALUES (?, ?, ?, ?, ?)",
            (date, source_currency, target_currency, rate, ts)
        )
        return cursor.lastrowid

def fetch_fx_rates(limit: int = 100) -> List[Dict]:
    """Fetch last N FX rates."""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM fx_rates ORDER BY timestamp DESC LIMIT ?",
            (limit,)
        ).fetchall()
        return [dict(row) for row in rows]

# -----------------------------
# Helper to fetch latest FX rate for a currency pair
# -----------------------------
def get_latest_fx(source_currency: str, target_currency: str) -> Optional[Dict]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM fx_rates WHERE source_currency=? AND target_currency=? ORDER BY timestamp DESC LIMIT 1",
            (source_currency, target_currency)
        ).fetchone()
        return dict(row) if row else None