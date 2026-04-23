"""
database/places/table.py
~~~~~~~~~~~~~~~~~~~~~~~~

Schema for the places table — a central geographic reference used by all
other tables that record "where" (location, health, transactions, etc.).

Points are snapped to a ~111m grid (3 d.p.) to allow fuzzy joins without
triggering duplicate geocode lookups for nearby fixes.

Storage strategy for Nominatim's variable response:
  - A small set of stable columns (country_code, country, display_name,
    timezone) are kept as indexable flat columns.
  - `locality`  — derived "best place name" picked from town/city/village/
                  etc. at geocode time using a preference chain.
  - `region`    — derived broader region picked from state/province/etc.
  - `raw_json`  — full Nominatim response, stored for re-processing if the
                  preference chain is ever revised without re-geocoding.
  - `city`, `suburb`, `road` kept for backward compatibility; new code
    should prefer `locality`.

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
                    region        TEXT,       -- derived: state/province/county
                    locality      TEXT,       -- derived: town/city/village/...
                    city          TEXT,       -- raw Nominatim field (may be NULL)
                    suburb        TEXT,
                    road          TEXT,
                    display_name  TEXT,
                    raw_json      TEXT,       -- full Nominatim response
                    geocoded_at   TEXT,
                    timezone      TEXT,
                    created_at    TEXT NOT NULL
                        DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
                    UNIQUE(lat_snap, lon_snap)
                );
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_places_country
                    ON places(country_code)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_places_city
                    ON places(city)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_places_locality
                    ON places(locality)
            """)

    def insert(self, record: PlacesRecord) -> None:
        """Insert a snapped coordinate. Idempotent on (lat_snap, lon_snap)."""
        with get_conn() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO places (lat_snap, lon_snap) VALUES (?, ?)",
                (record.lat_snap, record.lon_snap),
            )


table = PlacesTable()