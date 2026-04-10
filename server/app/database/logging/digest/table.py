"""
database/logging/digest/table.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Persistence layer for the daily log digest.

The DailyDigestHandler (config/logging.py) writes WARNING+ records here via a
background thread.  A scheduled task (scheduled_tasks/send_warn_error_log.py)
calls fetch_and_clear() once per day to drain the table and send the email.
"""

from dataclasses import dataclass

from database.base import BaseTable
from database.connection import get_conn


@dataclass
class LogDigestRecord:
    ts: str
    level: str
    logger_name: str
    module: str
    lineno: int
    message: str


class LogDigestTable(BaseTable[LogDigestRecord]):

    def init(self) -> None:
        """Create the log_digest table if it does not already exist."""
        with get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS log_digest (
                    id        INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts        TEXT NOT NULL,
                    level     TEXT NOT NULL,
                    logger    TEXT NOT NULL,
                    module    TEXT NOT NULL,
                    lineno    INTEGER NOT NULL,
                    message   TEXT NOT NULL
                )
            """)

    def insert(self, record: LogDigestRecord) -> None:
        """Insert a single log digest record."""
        with get_conn() as conn:
            conn.execute(
                "INSERT INTO log_digest (ts, level, logger, module, lineno, message) VALUES (?, ?, ?, ?, ?, ?)",
                (record.ts, record.level, record.logger_name, record.module, record.lineno, record.message),
            )
            conn.commit()

    def fetch_and_clear(self) -> list[LogDigestRecord]:
        """Atomically read all pending records and delete them from the table.

        Returns a list of LogDigestRecord. If a subsequent send fails, the caller
        is responsible for re-inserting them via restore().
        """
        with get_conn() as conn:
            rows = conn.execute(
                "SELECT ts, level, logger, module, lineno, message FROM log_digest ORDER BY id"
            ).fetchall()

            if not rows:
                return []
            
            conn.execute("DELETE FROM log_digest")
        return [LogDigestRecord(ts=r[0], level=r[1], logger_name=r[2], module=r[3], lineno=r[4], message=r[5]) for r in rows]

    def restore(self, records: list[LogDigestRecord]) -> None:
        """Re-insert records that failed to send (rollback on send failure)."""
        with get_conn() as conn:
            conn.executemany(
                "INSERT INTO log_digest (ts, level, logger, module, lineno, message) VALUES (?, ?, ?, ?, ?, ?)",
                [(r.ts, r.level, r.logger_name, r.module, r.lineno, r.message) for r in records],
            )


table = LogDigestTable()
