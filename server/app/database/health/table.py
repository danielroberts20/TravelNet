"""
database/health/table.py
~~~~~~~~~~~~~~~~~~~~~~~~
Schema for the health_quantity table.

health_quantity stores one row per (timestamp, metric, source) for all
single-value Apple Health metrics (step count, HRV, resting HR, etc.).
Heart Rate (min/avg/max triplet) lives in health_heart_rate; sleep stages
live in health_sleep.
"""

from database.connection import get_conn, to_iso_str


def init() -> None:
    """Create the health_quantity table and its indexes if they do not exist."""
    with get_conn() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS health_quantity (
            id          INTEGER PRIMARY KEY,
            timestamp   TEXT NOT NULL,
            metric      TEXT NOT NULL,
            value       REAL NOT NULL,
            unit        TEXT NOT NULL,
            source      TEXT,
            place_id    INTEGER REFERENCES places(id),
            created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
            UNIQUE(timestamp, metric, source)
        );
        """)

        conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_hquantity_timestamp
            ON health_quantity(timestamp);
        """)

        conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_hquantity_metric
            ON health_quantity(metric);
        """)

        conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_hquantity_source
            ON health_quantity(source);
        """)


def insert_health_quantity(timestamp: int, metric: str, value: float, unit: str, source: str | None = None) -> None:
    """Insert a single-value health metric row. Idempotent on (timestamp, metric, source)."""
    new_ts = to_iso_str(timestamp)
    with get_conn() as conn:
        conn.execute("""
            INSERT OR IGNORE INTO health_quantity (timestamp, metric, value, unit, source)
            VALUES (?, ?, ?, ?, ?)
        """, (new_ts, metric, value, unit, source))