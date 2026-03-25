"""
database/location/table.py
~~~~~~~~~~~~~~~~~~~~~~~~~~
Schema and insert helpers for the location_history table (Shortcuts CSV path)
and the location_unified view that merges Shortcuts and Overland data.
"""

import sqlite3
from typing import Dict, List, Optional

from database.util import get_conn, to_iso_str


def init() -> None:
    """Create the location_history table and its indexes if they do not exist."""
    with get_conn() as conn:
        # Locations table
        conn.execute("""
        CREATE TABLE IF NOT EXISTS location_history (
            id INTEGER PRIMARY KEY,

            -- Core temporal data
            timestamp TEXT NOT NULL,

            -- Geographic identifiers
            latitude REAL NOT NULL CHECK(latitude BETWEEN -90 AND 90),
            longitude REAL NOT NULL CHECK(longitude BETWEEN -180 AND 180),
            altitude REAL,                       -- meters
            activity TEXT,     

            -- Device data      
            device TEXT NOT NULL,                         -- e.g. "Mac", "iPhone"
            is_locked BOOLEAN,
            battery INTEGER CHECK(battery >= 0 AND battery <= 100),
            is_charging BOOLEAN,
            is_connected_charger BOOLEAN,
                        
            -- Network data      
            BSSID TEXT,
            RSSI INTEGER,

            created_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
                        
            UNIQUE(timestamp, device)
        );""")

        # Indexes for performance
        conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_location_timestamp
            ON location_history(timestamp);
        """)

        conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_location_device
            ON location_history(device);
        """)

        conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_location_lat_lon
            ON location_history(latitude, longitude);
        """)
    
def init_unified_view():
    """Create the location_unified view merging Overland and Shortcuts sources."""
    with get_conn() as conn:
        conn.execute("""
            CREATE VIEW IF NOT EXISTS location_unified AS
            SELECT
                timestamp                    AS timestamp,
                lat                          AS lat,
                lon                          AS lon,
                altitude                     AS altitude,
                activity                     AS activity,
                battery_level                AS battery,
                speed                        AS speed,
                device_id                    AS device,
                horizontal_accuracy          AS accuracy,
                'overland'                   AS source
            FROM location_overland

            UNION ALL

            SELECT
                timestamp                        AS timestamp,
                latitude                         AS lat,
                longitude                        AS lon,
                altitude                         AS altitude,
                activity                         AS activity,
                CAST(battery AS REAL) / 100.0    AS battery,
                NULL                             AS speed,
                device                           AS device,
                NULL                             AS accuracy,
                'shortcuts'                      AS source

            FROM location_history
            ORDER BY timestamp ASC
        """)

def insert_location(conn: sqlite3.Connection, timestamp: int, timezone: str, latitude: float, longitude: float, altitude: Optional[float],
                    activity: Optional[str], device: str, is_locked: Optional[bool], battery: Optional[int],
                    is_charging: Optional[bool], is_connected_charger: Optional[bool], BSSID: Optional[str], RSSI: Optional[int]):
    """Insert a location_history row and return its id (existing or newly inserted).

    Uses INSERT OR IGNORE on UNIQUE(timestamp, device) so re-processing the
    same CSV is idempotent.  Returns the row id for use when inserting the
    associated cellular_state rows.
    """
    new_ts = to_iso_str(timestamp)
    with conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO location_history (
                timestamp, 
                latitude, longitude, altitude, 
                activity, device, is_locked, 
                battery, is_charging, is_connected_charger, 
                BSSID, RSSI
                ) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (new_ts,
                latitude,
                longitude,
                altitude,
                activity,
                device,
                is_locked,
                battery,
                is_charging,
                is_connected_charger,
                BSSID,
                RSSI)
        )

        # Retrieve location_id (existing or newly inserted)
        cursor.execute("""
            SELECT id FROM location_history
            WHERE timestamp = ? AND device = ?
        """, (new_ts, device))

        row = cursor.fetchone()
        return row[0]

def fetch_locations(limit: int = 100) -> List[Dict]:
    """Fetch last N locations."""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM location_history ORDER BY timestamp DESC LIMIT ?",
            (limit,)
        ).fetchall()
        return [dict(row) for row in rows]