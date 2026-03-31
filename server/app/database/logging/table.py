"""
database/logging/table.py
~~~~~~~~~~~~~~~~~~~~~~~~~
Persistence layer for the daily log digest.

The DailyDigestHandler (config/logging.py) writes WARNING+ records here via a
background thread.  A scheduled task (scheduled_tasks/send_warn_error_log.py)
calls flush_and_clear() once per day to drain the table and send the email.
"""

from database.connection import get_conn


def init():
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


def insert_log_digest(record: tuple) -> None:
    """Insert a single log record tuple (ts, level, logger, module, lineno, message)."""
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO log_digest (ts, level, logger, module, lineno, message) VALUES (?, ?, ?, ?, ?, ?)",
            record,
        )
        conn.commit()


def fetch_and_clear() -> list:
    """Atomically read all pending records and delete them from the table.

    Returns the list of rows as tuples.  If a subsequent send fails, the caller
    is responsible for re-inserting them (see DailyDigestHandler.flush_and_send).
    """
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT ts, level, logger, module, lineno, message FROM log_digest ORDER BY id"
        ).fetchall()
    if not rows:
        return []
    with get_conn() as conn:
        conn.execute("DELETE FROM log_digest")
    return [tuple(r) for r in rows]


def restore_log_digest(rows: list) -> None:
    """Re-insert rows that failed to send (rollback on send failure)."""
    with get_conn() as conn:
        conn.executemany(
            "INSERT INTO log_digest (ts, level, logger, module, lineno, message) VALUES (?, ?, ?, ?, ?, ?)",
            rows,
        )
