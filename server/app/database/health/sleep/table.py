"""
database/health/sleep/table.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Schema and insert helpers for the health_sleep table.

Sleep Analysis is unaggregated — one row per stage interval. The start_ts
column corresponds to the HAE export timestamp field (converted to ISO 8601
UTC on ingest); end_ts is derived from the Unix epoch 'end' field in the
raw blob.
"""

from dataclasses import dataclass

from database.base import BaseTable
from database.connection import get_conn, to_iso_str


@dataclass
class SleepRecord:
    start_ts: int
    end_ts: int
    stage: str
    duration_hr: float
    source: str | None = None


class SleepTable(BaseTable[SleepRecord]):

    def init(self) -> None:
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

    def insert(self, record: SleepRecord) -> None:
        """Insert a single sleep stage interval. Idempotent on (start_ts, stage, source)."""
        start = to_iso_str(record.start_ts)
        end = to_iso_str(record.end_ts)
        with get_conn() as conn:
            conn.execute("""
                INSERT OR IGNORE INTO health_sleep (start_ts, end_ts, stage, duration_hr, source)
                VALUES (?, ?, ?, ?, ?)
            """, (start, end, record.stage, record.duration_hr, record.source))


table = SleepTable()
