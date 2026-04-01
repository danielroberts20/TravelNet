"""
database/cellular/table.py
~~~~~~~~~~~~~~~~~~~~~~~~~~
Schema and insert helpers for the cellular_state table.

Each row captures the cellular network state (carrier, radio technology,
roaming status) at a specific location fix.  A location fix may have multiple
cellular states if the device was connected to more than one carrier.
"""

import sqlite3

from models.telemetry import CellularState
from database.connection import get_conn


def init() -> None:
    """Create the cellular_state table and its indexes if they do not exist."""
    with get_conn() as conn:
        # Cellular state table
        conn.execute("""
        CREATE TABLE IF NOT EXISTS cellular_state (
            id INTEGER PRIMARY KEY,
            shortcut_id INTEGER NOT NULL,
            provider_name TEXT,
            radio TEXT,
            code TEXT,
            is_roaming BOOLEAN,
            FOREIGN KEY(shortcut_id) REFERENCES location_shortcuts(id) ON DELETE CASCADE,

            UNIQUE(shortcut_id, provider_name, radio)
        );""")

        conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_cellular_location
            ON cellular_state(shortcut_id);
        """)


def insert_cellular_state(
    conn: sqlite3.Connection,
    cellular_states: list[CellularState] | None,
    shortcut_id: int,
) -> None:
    """Insert one cellular state row per carrier for the given location fix.

    Uses INSERT OR IGNORE against UNIQUE(shortcut_id, provider_name, radio) so
    re-processing the same CSV row is idempotent.  Uses the supplied connection
    so the insert runs inside the same transaction as the parent location row.
    """
    if not cellular_states:
        return
    for cs in cellular_states:
        conn.execute(
            """
            INSERT OR IGNORE INTO cellular_state
                (shortcut_id, provider_name, radio, code, is_roaming)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                shortcut_id,
                cs.provider_name,
                cs.radio,
                cs.code,
                cs.is_roaming,
            ),
        )
