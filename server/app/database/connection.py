"""
database/connection.py
~~~~~~~~~~~~~~~~~~~~~~
Low-level SQLite connection and maintenance helpers.

get_conn() is the single point through which all DB access flows — it enables
WAL mode and foreign-key enforcement on every connection so callers don't have
to remember to set those pragmas themselves.
"""

import logging
import sqlite3
from datetime import datetime, timezone

from config.general import DATABASE_BACKUP_DIR, DB_FILE

logger = logging.getLogger(__name__)


def get_conn(read_only=False) -> sqlite3.Connection:
    """Open and return a new SQLite connection to the travel.db file.

    Parameters
    ----------
    read_only:  When True, opens the DB in URI read-only mode so no write-ahead
                log entries are created (safe for concurrent reporting queries).

    The connection always has:
      - row_factory = sqlite3.Row   (column access by name)
      - PRAGMA foreign_keys = ON
      - PRAGMA journal_mode = WAL   (write connections only)
    """
    if read_only:
        conn = sqlite3.connect(f"file:{DB_FILE}?mode=ro", uri=True)
    else:
        conn = sqlite3.connect(DB_FILE, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    if not read_only:
        conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def backup_db(prefix: str = None, suffix: str = None) -> str:
    """Create a point-in-time copy of travel.db in DATABASE_BACKUP_DIR.

    Uses SQLite's built-in online backup API so the copy is consistent even
    while the DB is being written to.  Returns the Path of the new backup file.
    """
    now = datetime.now()
    prefix = "" if prefix is None else prefix + "_"
    suffix = "" if suffix is None else "_" + suffix
    backup_path = DATABASE_BACKUP_DIR / f"{prefix}{now.strftime('%Y-%m-%d_%H-%M-%S')}{suffix}.db"
    source = sqlite3.connect(DB_FILE)
    dest   = sqlite3.connect(backup_path)
    source.backup(dest)
    dest.close()
    source.close()
    logger.info(f"DB backup created: {backup_path}")
    return backup_path



def increment_api_usage(service: str = "exchangerate.host"):
    """Increment the API usage count for the current month by 1."""
    month = datetime.now().strftime("%Y-%m")
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO api_usage (service, count, month)
            VALUES (?, 1, ?)
            ON CONFLICT(service) DO UPDATE SET count = count + 1
        """, (service, month))

def to_iso_str(s: str | int | float | datetime) -> str:
    """Convert any datetime to a ISO 8601 UTC string with Z suffix."""
    if isinstance(s, (int, float)):
        dt = datetime.fromtimestamp(s, tz=timezone.utc)
    elif isinstance(s, str):
        dt = datetime.fromisoformat(s)
    else:
        dt = s

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
