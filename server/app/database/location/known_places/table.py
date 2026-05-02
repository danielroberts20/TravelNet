"""
database/location/known_places/table.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Schema and CRUD helpers for the known_places and place_visits tables.

known_places stores cluster centroids of stay locations detected by the
location-change trigger.  place_visits records individual visits (arrived_at /
departed_at) for each known place.  place_visits references known_places(id),
so both tables are initialised together here.

The trigger logic that writes to these tables lives in triggers/location_change.py.
This module owns only the schema and the low-level insert/update helpers.
"""

from dataclasses import dataclass
from typing import Optional

from database.base import BaseTable
from database.connection import get_conn


@dataclass
class KnownPlaceRecord:
    latitude: float
    longitude: float
    first_seen: str         # ISO 8601 UTC
    place_id: Optional[int] = None
    label: Optional[str] = None
    notes: Optional[str] = None


class KnownPlacesTable(BaseTable[KnownPlaceRecord]):

    def init(self) -> None:
        """Create the known_places and place_visits tables if they do not exist."""
        with get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS known_places (
                    id               INTEGER PRIMARY KEY AUTOINCREMENT,
                    label            TEXT,
                    notes            TEXT,
                    latitude         REAL NOT NULL,
                    longitude        REAL NOT NULL,
                    place_id         INTEGER REFERENCES places(id),
                    first_seen       TEXT NOT NULL,
                    last_visited     TEXT,
                    visit_count      INTEGER NOT NULL DEFAULT 0,
                    total_time_mins  INTEGER NOT NULL DEFAULT 0,
                    current_visit_id INTEGER REFERENCES place_visits(id)
                );
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS place_visits (
                    id             INTEGER PRIMARY KEY AUTOINCREMENT,
                    known_place_id INTEGER NOT NULL REFERENCES known_places(id) ON DELETE CASCADE,
                    arrived_at     TEXT NOT NULL,
                    departed_at    TEXT,
                    duration_mins  INTEGER
                );
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_known_places_lat_lon
                    ON known_places(latitude, longitude);
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_place_visits_known_place_id
                    ON place_visits(known_place_id);
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_place_visits_arrived
                    ON place_visits(arrived_at);
            """)

    def insert(self, record: KnownPlaceRecord) -> int:
        """Insert a new known place and return its id."""
        with get_conn() as conn:
            cursor = conn.execute("""
                INSERT INTO known_places (latitude, longitude, place_id, first_seen, visit_count, last_visited, label, notes)
                VALUES (?, ?, ?, ?, 1, ?, ?)
            """, (record.latitude, record.longitude, record.place_id, record.first_seen, record.first_seen, record.label, record.notes))
            return cursor.lastrowid

    def label_place(self, place_id: int, label: str) -> bool:
        """Set or update the human-readable label for a known place. Returns True if found."""
        with get_conn() as conn:
            cursor = conn.execute(
                "UPDATE known_places SET label = ? WHERE id = ?",
                (label, place_id),
            )
            return cursor.rowcount > 0
    
    def add_note_place(self, place_id: int, note: str)-> bool:
        """Set or update the human-readable notes for a known place. Returns True if found."""
        with get_conn() as conn:
            cursor = conn.execute(
                "UPDATE known_places SET notes = ? WHERE id = ?",
                (note, place_id),
            )
            return cursor.rowcount > 0

    def insert_visit(self, place_id: int, arrived_at: str) -> int:
        """Open a new visit record for a known place and return its id."""
        with get_conn() as conn:
            cursor = conn.execute("""
                INSERT INTO place_visits (known_place_id, arrived_at)
                VALUES (?, ?)
            """, (place_id, arrived_at))
            return cursor.lastrowid

    def set_current_visit(self, place_id: int, visit_id: int) -> None:
        """Point known_places.current_visit_id at the active visit."""
        with get_conn() as conn:
            conn.execute(
                "UPDATE known_places SET current_visit_id = ? WHERE id = ?",
                (visit_id, place_id),
            )

    def close_visit(self, visit_id: int, place_id: int, departed_at: str, duration_mins: int) -> None:
        """Record departure time, accumulate total time, and clear current_visit_id."""
        with get_conn() as conn:
            conn.execute("""
                UPDATE place_visits
                SET departed_at = ?, duration_mins = ?
                WHERE id = ?
            """, (departed_at, duration_mins, visit_id))

            conn.execute("""
                UPDATE known_places
                SET total_time_mins = total_time_mins + ?,
                    current_visit_id = NULL
                WHERE id = ?
            """, (duration_mins, place_id))

    def increment_visit_count(self, place_id: int, last_visited: str, visit_id: int) -> None:
        """Increment visit_count and set current_visit_id for a return visit."""
        with get_conn() as conn:
            conn.execute("""
                UPDATE known_places
                SET visit_count = visit_count + 1,
                    last_visited = ?,
                    current_visit_id = ?
                WHERE id = ?
            """, (last_visited, visit_id, place_id))


table = KnownPlacesTable()
