import sqlite3
from typing import Dict, List, Optional

from database.util import get_conn

def init():
    with get_conn() as conn:
        # Locations table
        conn.execute("""
        CREATE TABLE IF NOT EXISTS location_history (
            id INTEGER PRIMARY KEY,

            -- Core temporal data
            timestamp INTEGER NOT NULL CHECK(timestamp > 1500000000),  -- Unix timestamp (seconds)
            timezone TEXT,                       -- e.g. "+0000, -0300"

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

            created_at INTEGER DEFAULT (strftime('%s','now')),
                        
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

def insert_location(conn: sqlite3.Connection, timestamp: int, timezone: str, latitude: float, longitude: float, altitude: Optional[float],
                    activity: Optional[str], device: str, is_locked: Optional[bool], battery: Optional[int],
                    is_charging: Optional[bool], is_connected_charger: Optional[bool], BSSID: Optional[str], RSSI: Optional[int]):
    with conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO location_history (
                timestamp, timezone, 
                latitude, longitude, altitude, 
                activity, device, is_locked, 
                battery, is_charging, is_connected_charger, 
                BSSID, RSSI
                ) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (timestamp,
                timezone,
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
        """, (timestamp, device))

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