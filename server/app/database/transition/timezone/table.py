"""
database/location/table.py
~~~~~~~~~~~~~~~~~~~~~~~~~~
Schema and insert helpers for the location_shortcuts table (Shortcuts CSV path)
and the location_unified view that merges Shortcuts and Overland data.

LocationShortcutsTable.insert() returns the row id so the caller can
immediately pass it to CellularTable.insert_batch() — no shared connection
needed between the two tables.
"""

from dataclasses import dataclass
from typing import Optional

from database.base import BaseTable
from database.connection import get_conn


@dataclass
class TransitionTimezoneRecord:
    transitioned_at: str  # timestamp of first location point in new tz
    from_tz: Optional[str]  # NULL for first record
    to_tz: str  # e.g. "Pacific/Auckland"
    from_offset: Optional[str]  # e.g. "+00:00"
    to_offset: str  # e.g. "+13:00"


class TransitionTimezoneTable(BaseTable[TransitionTimezoneRecord]):

    def init(self) -> None:
        """Create the transition_timezone table and its indexes"""
        with get_conn() as conn:
            conn.execute("""
            CREATE TABLE IF NOT EXISTS transition_timezone (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                transitioned_at TEXT NOT NULL,   -- timestamp of first location point in new tz
                from_tz      TEXT,               -- NULL for first record
                to_tz        TEXT NOT NULL,      -- e.g. "Pacific/Auckland"
                from_offset  TEXT,               -- e.g. "+00:00"
                to_offset    TEXT NOT NULL,
                place_id     INTEGER REFERENCES places(id),
                UNIQUE(transitioned_at, to_tz)
            );""")

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_transition_timezone_transitioned_at ON transition_timezone(transitioned_at);
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_transition_timezone_place_id ON transition_timezone(place_id);
            """)

    def insert(self, record: TransitionTimezoneRecord) -> int:
        """Insert a transition_timezone row and return its id (existing or newly inserted).

        Uses INSERT OR IGNORE on UNIQUE(from_tz, to_tz, transitioned_at) so re-processing
        the same transition is idempotent.
        """

        with get_conn() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO transition_timezone (transitioned_at, from_tz, to_tz, from_offset, to_offset)
                VALUES (?, ?, ?, ?, ?);
                """,
                (record.transitioned_at, record.from_tz, record.to_tz, record.from_offset, record.to_offset),
            )
            return conn.execute("SELECT changes()").fetchone()[0] > 0


table = TransitionTimezoneTable()
