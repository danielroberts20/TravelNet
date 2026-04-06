"""
database/location/table.py
~~~~~~~~~~~~~~~~~~~~~~~~~~
Schema and insert helpers for the location_shortcuts table (Shortcuts CSV path)
and the location_unified view that merges Shortcuts and Overland data.
"""

import sqlite3
from typing import Dict, List, Optional

from database.connection import get_conn, to_iso_str
from database.location.util import get_place_id


def init() -> None:
    """Create the location_shortcuts table and its indexes if they do not exist."""
    with get_conn() as conn:
        # Locations table
        conn.execute("""
        CREATE TABLE IF NOT EXISTS location_shortcuts (
            id                    INTEGER PRIMARY KEY,
            timestamp             TEXT NOT NULL,
            latitude              REAL NOT NULL CHECK(latitude  BETWEEN -90  AND 90),
            longitude             REAL NOT NULL CHECK(longitude BETWEEN -180 AND 180),
            altitude              REAL,
            device                TEXT NOT NULL,
            is_locked             INTEGER,
            battery               INTEGER CHECK(battery BETWEEN 0 AND 100),
            is_charging           INTEGER,
            is_connected_charger  INTEGER,
            bssid                 TEXT,
            rssi                  INTEGER,
            place_id              INTEGER REFERENCES places(id),
            created_at            TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
            UNIQUE(timestamp, device)
        );""")

        # Indexes for performance
        conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_lshortcuts_timestamp
            ON location_shortcuts(timestamp);
        """)

        conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_lshortcuts_device
            ON location_shortcuts(device);
        """)

        conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_lshortcuts_lat_lon
            ON location_shortcuts(latitude, longitude);
        """)

        conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_lshortcuts_place
            ON location_shortcuts(place_id);
        """)
    
def init_unified_view():
    """Create the location_unified view merging Overland and Shortcuts sources."""
    with get_conn() as conn:
        conn.execute("""
            CREATE VIEW IF NOT EXISTS location_unified AS
            SELECT
                'overland'            AS source,
                o.id                  AS source_id,
                o.timestamp,
                o.latitude,
                o.longitude,
                o.altitude,
                o.activity,
                o.battery_level       AS battery,
                o.speed,
                o.device_id           AS device,
                o.horizontal_accuracy AS accuracy,
                o.place_id
            FROM location_overland o

            UNION ALL

            SELECT
                'shortcuts'                      AS source,
                s.id                             AS source_id,
                s.timestamp,
                s.latitude,
                s.longitude,
                s.altitude,
                NULL                             AS activity,
                CAST(s.battery AS REAL) / 100.0  AS battery,
                NULL                             AS speed,
                s.device,
                NULL                             AS accuracy,
                s.place_id
            FROM location_shortcuts s

            ORDER BY timestamp ASC
        """)

def insert_location(conn: sqlite3.Connection, timestamp: int, latitude: float, longitude: float, altitude: Optional[float],
                    device: str, is_locked: Optional[bool], battery: Optional[int],
                    is_charging: Optional[bool], is_connected_charger: Optional[bool], BSSID: Optional[str], RSSI: Optional[int]):
    """Insert a location_shortcuts row and return its id (existing or newly inserted).

    Uses INSERT OR IGNORE on UNIQUE(timestamp, device) so re-processing the
    same CSV is idempotent.  Returns the row id for use when inserting the
    associated cellular_state rows.
    """

    new_ts = to_iso_str(timestamp)
    place_id = get_place_id(latitude, longitude)

    
    with conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO location_shortcuts (
                timestamp, 
                latitude, longitude, altitude, 
                device, is_locked, 
                battery, is_charging, is_connected_charger, 
                bssid, rssi, place_id
                ) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (new_ts,
                latitude,
                longitude,
                altitude,
                device,
                is_locked,
                battery,
                is_charging,
                is_connected_charger,
                BSSID,
                RSSI,
                place_id)
        )

        # Retrieve location_id (existing or newly inserted)
        cursor.execute("""
            SELECT id FROM location_shortcuts
            WHERE timestamp = ? AND device = ?
        """, (new_ts, device))

        row = cursor.fetchone()
        return row[0]

def fetch_locations(limit: int = 100) -> List[Dict]:
    """Fetch last N locations."""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM location_shortcuts ORDER BY timestamp DESC LIMIT ?",
            (limit,)
        ).fetchall()
        return [dict(row) for row in rows]