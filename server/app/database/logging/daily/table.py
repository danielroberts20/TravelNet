"""
database/logging/daily/table.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Persistence layer for the daily cron results.

The DailyCronHandler (config/logging.py) writes cron results here via a
background thread.  A scheduled task (scheduled_tasks/send_cron_results.py)
calls fetch_and_clear() once per day to drain the table and send the email.
"""

from dataclasses import dataclass

from database.base import BaseTable
from database.connection import get_conn


@dataclass
class DailyCronRecord:
    job_name: str
    ran_at: str        # UTC timestamp of completion
    date: str          # local date, for grouping (e.g. "2026-09-15")
    success: bool
    duration_s: float | None
    metrics: str       # JSON blob of job.add_metric() calls
    error: str | None  # traceback if failed


class DailyCronTable(BaseTable[DailyCronRecord]):

    def init(self) -> None:
        """Create the cron_results table if it does not already exist."""
        with get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS cron_results (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_name    TEXT NOT NULL,
                    ran_at      TEXT NOT NULL,        -- UTC timestamp of completion
                    date        TEXT NOT NULL,        -- local date, for grouping (e.g. "2026-09-15")
                    success     INTEGER NOT NULL,     -- 1 or 0
                    duration_s  REAL,
                    metrics     TEXT,                 -- JSON blob of job.add_metric() calls
                    error       TEXT,                 -- traceback if failed
                    UNIQUE(job_name, date)            -- one result per job per day; OR REPLACE on re-run
                );""")

    def insert(self, record: DailyCronRecord) -> None:
        """Insert a single daily cron record."""
        with get_conn() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO cron_results (job_name, ran_at, date, success, duration_s, metrics, error) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (record.job_name, record.ran_at, record.date, int(record.success), record.duration_s, record.metrics, record.error),
            )
            conn.commit()

    def fetch_and_clear(self) -> list[DailyCronRecord]:
        """Atomically read all pending records and delete them from the table.

        Returns a list of DailyCronRecord. If a subsequent send fails, the caller
        is responsible for re-inserting them via restore().
        """
        with get_conn() as conn:
            rows = conn.execute(
                "SELECT job_name, ran_at, date, success, duration_s, metrics, error FROM cron_results ORDER BY id"
            ).fetchall()

            if not rows:
                return []
            
            conn.execute("DELETE FROM cron_results")
        return [DailyCronRecord(job_name=r[0], ran_at=r[1], date=r[2], success=bool(r[3]), duration_s=r[4], metrics=r[5], error=r[6]) for r in rows]

    def restore(self, records: list[DailyCronRecord]) -> None:
        """Re-insert records that failed to send (rollback on send failure)."""
        with get_conn() as conn:
            conn.executemany(
                "INSERT INTO cron_results (job_name, ran_at, date, success, duration_s, metrics, error) VALUES (?, ?, ?, ?, ?, ?, ?)",
                [(r.job_name, r.ran_at, r.date, int(r.success), r.duration_s, r.metrics, r.error) for r in records],
            )


table = DailyCronTable()
