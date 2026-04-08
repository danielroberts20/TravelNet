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
from database.connection import get_conn, to_iso_str
from database.location.geocoding import get_place_id


@dataclass
class LocationRecord:
    timestamp: int
    latitude: float
    longitude: float
    device: str
    altitude: Optional[float] = None
    is_locked: Optional[bool] = None
    battery: Optional[int] = None
    is_charging: Optional[bool] = None
    is_connected_charger: Optional[bool] = None
    bssid: Optional[str] = None
    rssi: Optional[int] = None


class LocationShortcutsTable(BaseTable[LocationRecord]):

    def init(self) -> None:
        """Create the location_shortcuts table, its indexes, and the unified view."""
        with get_conn() as conn:
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

    def init_unified_view(self) -> None:
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

    def insert(self, record: LocationRecord) -> int:
        """Insert a location_shortcuts row and return its id (existing or newly inserted).

        Uses INSERT OR IGNORE on UNIQUE(timestamp, device) so re-processing
        the same CSV is idempotent. Returns the row id for use when inserting
        the associated cellular_state rows.
        """
        ts = to_iso_str(record.timestamp)
        place_id = get_place_id(record.latitude, record.longitude)

        with get_conn() as conn:
            conn.execute("""
                INSERT OR IGNORE INTO location_shortcuts (
                    timestamp,
                    latitude, longitude, altitude,
                    device, is_locked,
                    battery, is_charging, is_connected_charger,
                    bssid, rssi, place_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                ts,
                record.latitude, record.longitude, record.altitude,
                record.device, record.is_locked,
                record.battery, record.is_charging, record.is_connected_charger,
                record.bssid, record.rssi, place_id,
            ))

            row = conn.execute("""
                SELECT id FROM location_shortcuts
                WHERE timestamp = ? AND device = ?
            """, (ts, record.device)).fetchone()

            return row[0]


table = LocationShortcutsTable()
