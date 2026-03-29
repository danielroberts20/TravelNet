"""
database/cellular/table.py
~~~~~~~~~~~~~~~~~~~~~~~~~~
Schema and insert helpers for the cellular_state table.

Each row captures the cellular network state (carrier, radio technology,
roaming status) at a specific location fix.  A location fix may have multiple
cellular states if the device was connected to more than one carrier.
"""

from logging import log
import sqlite3
from typing import Optional

from models.telemetry import CellularState
from database.util import get_conn


def init() -> None:
    """Create the cellular_state table and its indexes if they do not exist."""
    with get_conn() as conn:
        # Cellular state table
        conn.execute("""
        CREATE TABLE IF NOT EXISTS cellular_state (
            id INTEGER PRIMARY KEY,
            location_id INTEGER NOT NULL,
            provider_name TEXT,
            radio TEXT,
            code TEXT,
            is_roaming BOOLEAN,
            FOREIGN KEY(location_id) REFERENCES location_history(id) ON DELETE CASCADE,

            UNIQUE(location_id, provider_name, radio)
        );""")

        conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_cellular_location
            ON cellular_state(location_id);
        """)


def insert_cellular_state(
    conn: sqlite3.Connection,
    cellular_states: list[CellularState],
    location_id: int,
) -> None:
    """Insert one cellular state row per carrier for the given location fix.

    Uses INSERT OR IGNORE against UNIQUE(location_id, provider_name, radio) so
    re-processing the same CSV row is idempotent.
    """
    with get_conn() as conn:
        for cs in cellular_states:
            conn.execute(
                """
                INSERT OR IGNORE INTO cellular_state
                    (location_id, provider_name, radio, code, is_roaming)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    location_id,
                    cs.provider_name,
                    cs.radio,
                    cs.code,
                    cs.is_roaming,
                ),
            )
