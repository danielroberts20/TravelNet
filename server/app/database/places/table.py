"""
database/places/table.py
~~~~~~~~~~~~~~~~~~~~~~~~
Schema for the places table — a central geographic reference used by all
other tables that record "where" (location, health, transactions, etc.).

Points are snapped to a ~111m grid (3 d.p.) to allow fuzzy joins without
triggering duplicate geocode lookups for nearby fixes.

place_visits lives in database/location/known_places/table.py because it
references the known_places FK target defined there.
"""

from dataclasses import dataclass

from database.base import BaseTable
from database.connection import get_conn


@dataclass
class PlacesRecord:
    lat_snap: float
    lon_snap: float


class PlacesTable(BaseTable[PlacesRecord]):

    def init(self) -> None:
        """Create the places table and its indexes if they do not exist."""
        with get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS places (
                    id            INTEGER PRIMARY KEY,
                    lat_snap      REAL NOT NULL,
                    lon_snap      REAL NOT NULL,
                    country_code  TEXT,
                    country       TEXT,
                    region        TEXT,
                    city          TEXT,
                    suburb        TEXT,
                    road          TEXT,
                    display_name  TEXT,
                    geocoded_at   TEXT,
                    timezone      TEXT,
                    created_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
                    UNIQUE(lat_snap, lon_snap)
                );
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_places_country
                    ON places(country_code);
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_places_city
                    ON places(city);
            """)

    def insert(self, record: PlacesRecord) -> None:
        """Insert a snapped coordinate into places. Idempotent on (lat_snap, lon_snap)."""
        with get_conn() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO places (lat_snap, lon_snap) VALUES (?, ?)",
                (record.lat_snap, record.lon_snap),
            )


table = PlacesTable()
