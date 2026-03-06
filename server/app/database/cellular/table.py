from logging import log
import sqlite3
from typing import Optional

from telemetry_models import CellularState
from database.util import get_conn

def init():
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

def insert_cellular_state(conn: sqlite3.Connection, cellular_states: list[CellularState], location_id: int):
    with get_conn() as conn:
        for cs in cellular_states:
                conn.execute("""
                    INSERT OR IGNORE INTO cellular_state (location_id, provider_name, radio, code, is_roaming) VALUES (?, ?, ?, ?, ?)""", 
                    (location_id,
                    cs.provider_name,
                    cs.radio,
                    cs.code,
                    cs.is_roaming))