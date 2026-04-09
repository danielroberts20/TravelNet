"""
database/triggers/table.py
~~~~~~~~~~~~~~~~~~~~~~~~~~
Schema and insert helpers for the trigger_log table.

trigger_log records every fired dispatch event with its trigger name,
timestamp, and payload. Used by dispatch() to enforce cooldown windows.
"""

from dataclasses import dataclass

from database.base import BaseTable
from database.connection import get_conn


@dataclass
class TriggerRecord:
    trigger: str
    fired_at: str          # ISO 8601 UTC string
    payload: str           # JSON-serialised payload dict
    place_id: int | None = None


class TriggerLogTable(BaseTable[TriggerRecord]):

    def init(self) -> None:
        """Create the trigger_log table if it does not exist."""
        with get_conn() as conn:
            conn.execute("""
            CREATE TABLE IF NOT EXISTS trigger_log (
                id          INTEGER PRIMARY KEY,
                trigger     TEXT NOT NULL,
                fired_at    TEXT NOT NULL,
                place_id    INTEGER REFERENCES places(id),
                payload     TEXT
            );""")

    def insert(self, record: TriggerRecord) -> None:
        """Insert a trigger log entry."""
        with get_conn() as conn:
            conn.execute(
                "INSERT INTO trigger_log (trigger, fired_at, place_id, payload) VALUES (?, ?, ?, ?)",
                (record.trigger, record.fired_at, record.place_id, record.payload),
            )


table = TriggerLogTable()
