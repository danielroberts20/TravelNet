"""
database/health/heart_rate/table.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Schema for the health_heart_rate table.

Heart Rate from Apple Health exports as a min/avg/max triplet per reading,
which does not fit the single-value health_quantity pattern, so it gets its
own table.
"""

from database.connection import get_conn, to_iso_str


def init() -> None:
    """Create the health_heart_rate table and its indexes if they do not exist."""
    with get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS health_heart_rate (
                id          INTEGER PRIMARY KEY,
                timestamp   TEXT NOT NULL,
                min_bpm     REAL NOT NULL,
                avg_bpm     REAL NOT NULL,
                max_bpm     REAL NOT NULL,
                source      TEXT,
                place_id    INTEGER REFERENCES places(id),
                created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
                UNIQUE(timestamp, source)
            );
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_hhr_timestamp
                ON health_heart_rate(timestamp);
        """)


def insert_heart_rate(timestamp: int, min_bpm: float, avg_bpm: float, max_bpm: float, source: str | None = None) -> None:
    """Insert a Heart Rate triplet row. Idempotent on (timestamp, source)."""
    new_ts = to_iso_str(timestamp)
    with get_conn() as conn:
        conn.execute("""
            INSERT OR IGNORE INTO health_heart_rate (timestamp, min_bpm, avg_bpm, max_bpm, source)
            VALUES (?, ?, ?, ?, ?)
        """, (new_ts, min_bpm, avg_bpm, max_bpm, source))
