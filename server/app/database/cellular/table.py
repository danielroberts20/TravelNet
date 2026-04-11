"""
database/cellular/table.py
~~~~~~~~~~~~~~~~~~~~~~~~~~
Schema and insert helpers for the cellular_state table.

Each row captures the cellular network state (carrier, radio technology,
roaming status) at a specific location fix.  A location fix may have multiple
cellular states if the device was connected to more than one carrier.

CellularTable.insert() manages its own connection. The caller (upload/location/
shortcuts.py) first inserts the parent location row to get its id, then calls
insert() or insert_batch() here. This is the sequential pattern agreed in place
of the previous shared-connection approach.
"""

from dataclasses import dataclass

from models.telemetry import CellularState
from database.base import BaseTable
from database.connection import get_conn


@dataclass
class CellularRecord:
    shortcut_id: int
    provider_name: str | None
    radio: str | None
    code: str | None
    is_roaming: bool | None


class CellularTable(BaseTable[CellularRecord]):

    def init(self) -> None:
        """Create the cellular_state table and its indexes if they do not exist."""
        with get_conn() as conn:
            conn.execute("""
            CREATE TABLE IF NOT EXISTS cellular_state (
                id INTEGER PRIMARY KEY,
                shortcut_id INTEGER NOT NULL, -- foreign key to location_shortcuts
                provider_name TEXT, -- cellular carrier name, e.g. "T-Mobile", "AT&T", "Verizon". May be null if not available.
                radio TEXT, -- cellular radio technology, e.g. "LTE", "NR5G". May be null if not available.
                code TEXT, -- country code or other carrier code. May be null if not available.
                is_roaming BOOLEAN, -- whether the device is roaming on this carrier. May be null if not available.
                FOREIGN KEY(shortcut_id) REFERENCES location_shortcuts(id) ON DELETE CASCADE, -- ensure cellular states are deleted when their parent location shortcut is deleted.
                UNIQUE(shortcut_id, provider_name, radio)
            );""")

            conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_cellular_shortcut
                ON cellular_state(shortcut_id);
            """)

    def insert(self, record: CellularRecord) -> None:
        """Insert a single cellular state row. Idempotent on (shortcut_id, provider_name, radio)."""
        with get_conn() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO cellular_state
                    (shortcut_id, provider_name, radio, code, is_roaming)
                VALUES (?, ?, ?, ?, ?)
                """,
                (record.shortcut_id, record.provider_name, record.radio, record.code, record.is_roaming),
            )

    def insert_batch(self, states: list[CellularState] | None, shortcut_id: int) -> None:
        """Insert all cellular state rows for a given location fix.

        Accepts the raw list[CellularState] from the telemetry model so the
        caller does not need to build CellularRecord objects manually.
        """
        if not states:
            return
        for cs in states:
            self.insert(CellularRecord(
                shortcut_id=shortcut_id,
                provider_name=cs.provider_name,
                radio=cs.radio,
                code=cs.code,
                is_roaming=cs.is_roaming,
            ))


table = CellularTable()
