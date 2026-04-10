"""
database/transition/country/table.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Schema and helpers for the country_transitions table.
 
Tracks when the user crosses into a new country, derived from
location_unified joined to places. Populated by the scheduled task
detect_country_transitions.py.
"""
 
from dataclasses import dataclass
from typing import Optional
 
from database.base import BaseTable
from database.connection import get_conn
 
 
@dataclass
class CountryTransitionRecord:
    country_code: str          # ISO 3166-1 alpha-2, e.g. "AU"
    country: str               # full name, e.g. "Australia"
    entered_at: str            # ISO 8601 UTC timestamp of first GPS point
    entry_lat: Optional[float]
    entry_lon: Optional[float]
    entry_place_id: Optional[int]
    departed_at: Optional[str] = None
 
 
class CountryTransitionTable(BaseTable[CountryTransitionRecord]):
 
    def init(self) -> None:
        """Create country_transitions and its indexes if they don't exist."""
        with get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS country_transitions (
                    id               INTEGER PRIMARY KEY AUTOINCREMENT,
                    country_code     TEXT NOT NULL,
                    country          TEXT NOT NULL,
                    entered_at       TEXT NOT NULL,
                    departed_at      TEXT,
                    entry_lat        REAL,
                    entry_lon        REAL,
                    entry_place_id   INTEGER REFERENCES places(id),
                    created_at       TEXT NOT NULL
                        DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
                    UNIQUE(country_code, entered_at)
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_ct_entered
                ON country_transitions(entered_at)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_ct_country
                ON country_transitions(country_code)
            """)
 
    def insert(self, record: CountryTransitionRecord) -> bool:
        """Insert a country transition row.
 
        Uses INSERT OR IGNORE — safe to re-run. Returns True if a new row
        was inserted, False if it already existed.
        """
        with get_conn() as conn:
            conn.execute("""
                INSERT OR IGNORE INTO country_transitions
                    (country_code, country, entered_at, departed_at,
                     entry_lat, entry_lon, entry_place_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                record.country_code,
                record.country,
                record.entered_at,
                record.departed_at,
                record.entry_lat,
                record.entry_lon,
                record.entry_place_id,
            ))
            return conn.execute("SELECT changes()").fetchone()[0] > 0
 
    def update_departed_at(self, country_code: str, departed_at: str) -> None:
        """Fill in the departure timestamp on the most recent open row for
        country_code (where departed_at IS NULL).
 
        A no-op if no open row exists (e.g. task is re-run after departure
        was already recorded).
        """
        with get_conn() as conn:
            conn.execute("""
                UPDATE country_transitions
                SET departed_at = ?
                WHERE country_code = ?
                  AND departed_at IS NULL
                ORDER BY entered_at DESC
                LIMIT 1
            """, (departed_at, country_code))
 
 
table = CountryTransitionTable()