"""
database/health/sleep/table.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Schema for the health_sleep table.

Sleep Analysis is unaggregated — one row per stage interval. The start_ts
column corresponds to the HAE export timestamp field (converted to ISO 8601
UTC on ingest); end_ts is derived from the Unix epoch 'end' field in the
raw blob.
"""

from database.connection import get_conn, to_iso_str


def init() -> None:
    """Create the health_sleep table and its indexes if they do not exist."""
    with get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS health_sleep (
                id          INTEGER PRIMARY KEY,
                start_ts    TEXT NOT NULL,
                end_ts      TEXT NOT NULL,
                stage       TEXT NOT NULL,
                duration_hr REAL NOT NULL,
                source      TEXT,
                created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
                UNIQUE(start_ts, stage, source)
            );
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_hsleep_start
                ON health_sleep(start_ts);
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_hsleep_stage
                ON health_sleep(stage);
        """)


def insert_sleep_stage(start_ts: int, end_ts: int, stage: str, duration_hr: float, source: str | None = None) -> None:
    """Insert a single sleep stage interval. Idempotent on (start_ts, stage, source)."""
    new_start = to_iso_str(start_ts)
    new_end = to_iso_str(end_ts)
    with get_conn() as conn:
        conn.execute("""
            INSERT OR IGNORE INTO health_sleep (start_ts, end_ts, stage, duration_hr, source)
            VALUES (?, ?, ?, ?, ?)
        """, (new_start, new_end, stage, duration_hr, source))
