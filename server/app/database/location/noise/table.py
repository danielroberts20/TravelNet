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
class LocationNoiseRecord:
    overland_id: int
    tier: int  # 1, 2, or 3
    reason: str  # 'accuracy_threshold', 'displacement_spike', 'cluster_outlier'
    flagged_at: Optional[str] = None  # timestamp when the point was flagged as noise; defaults to now


class LocationNoiseTable(BaseTable[LocationNoiseRecord]):

    def init(self) -> None:
        """Create the location_noise table, its indexes, and the cleaned view."""
        with get_conn() as conn:
            conn.execute("""
            CREATE TABLE IF NOT EXISTS location_noise (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                overland_id INTEGER NOT NULL REFERENCES location_overland(id) ON DELETE CASCADE,
                tier INTEGER NOT NULL,          -- 1, 2, or 3
                reason TEXT NOT NULL,           -- 'accuracy_threshold', 'displacement_spike', 'cluster_outlier'
                flagged_at TEXT NOT NULL DEFAULT(strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
            );""")

            conn.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_noise_overland_id ON location_noise(overland_id);
            """) # unique: a point gets one flag (the earliest tier that catches it)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_noise_tier ON location_noise(tier);
            """)

    def init_clean_view(self) -> None:
        """Create the location_unified view merging Overland and Shortcuts sources."""
        with get_conn() as conn:
            conn.execute("""
                CREATE VIEW IF NOT EXISTS location_overland_cleaned AS
                SELECT o.*
                FROM location_overland o
                WHERE NOT EXISTS (
                    SELECT 1 FROM location_noise n WHERE n.overland_id = o.id
                );
            """)

    def insert(self, record: LocationNoiseRecord) -> int:
        """Insert a location_shortcuts row and return its id (existing or newly inserted).

        Uses INSERT OR IGNORE on UNIQUE(timestamp, device) so re-processing
        the same CSV is idempotent. Returns the row id for use when inserting
        the associated cellular_state rows.
        """

        with get_conn() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO location_noise (overland_id, tier, reason)
                VALUES (?, ?, ?)
                """,
                (record.overland_id, record.tier, record.reason),
            )


table = LocationNoiseTable()
