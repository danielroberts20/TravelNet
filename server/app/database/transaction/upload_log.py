"""
database/transaction/upload_log.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Schema and upsert helper for the upload_log table.

Tracks which (source, calendar month) combinations have had a transaction
export uploaded, independent of whether that upload actually contained any
rows (row_count = 0 is a valid "attempted, found nothing" outcome). This is
the source of truth the transactions daily_summary domain uses to decide
spend_complete — a date is only complete once both Revolut and Wise have
confirmed coverage for the month it falls in.

Written by the /upload/revolut and /upload/wise endpoints (via the
background ingest tasks in upload/transaction/router.py) after ingestion
finishes, so row_count reflects the actual number of transactions parsed
from that upload. insert() always represents a genuine upload event, so it
always writes inferred=0 (and un-sets it on conflict) — the one-off
historical backfill (database/migrations/backfill_upload_log_and_completeness.py)
writes inferred=1 rows directly via raw SQL instead, since those never had
a real upload event to record.
"""

from dataclasses import dataclass

from database.base import BaseTable
from database.connection import get_conn


@dataclass
class UploadLogRecord:
    source:       str   # 'revolut' | 'wise'
    period_start: str   # ISO date, first day of covered month
    period_end:   str   # ISO date, last day of covered month
    row_count:    int   # rows ingested; 0 is a valid "attempted, found nothing"


class UploadLogTable(BaseTable[UploadLogRecord]):

    def init(self) -> None:
        with get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS upload_log (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    source        TEXT NOT NULL,
                    period_start  TEXT NOT NULL,
                    period_end    TEXT NOT NULL,
                    row_count     INTEGER NOT NULL,
                    -- 1 if this row was reconstructed retroactively by the historical
                    -- backfill script rather than recorded from a real upload event.
                    inferred      INTEGER NOT NULL DEFAULT 0,
                    -- NULL when inferred=1 (backfill never fabricates a real timestamp).
                    -- Genuine uploads (via insert()/the upload endpoints) always set it.
                    uploaded_at   TEXT,
                    UNIQUE(source, period_start, period_end)
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_upload_log_period
                ON upload_log(source, period_start, period_end)
            """)

    def insert(self, record: UploadLogRecord) -> None:
        """Upsert coverage for (source, period) from a genuine upload event.

        Re-uploads/corrections update row_count and uploaded_at rather than
        erroring — a source may legitimately re-upload the same month. Always
        writes inferred=0, promoting a previously-backfilled (inferred=1) row
        to a real one if this period was later actually uploaded.
        """
        with get_conn() as conn:
            conn.execute("""
                INSERT INTO upload_log (source, period_start, period_end, row_count, inferred, uploaded_at)
                VALUES (?, ?, ?, ?, 0, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
                ON CONFLICT(source, period_start, period_end) DO UPDATE SET
                    row_count   = excluded.row_count,
                    uploaded_at = excluded.uploaded_at,
                    inferred    = 0
            """, (record.source, record.period_start, record.period_end, record.row_count))


table = UploadLogTable()
