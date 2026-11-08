"""
database/location/sub_places/table.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Schema and CRUD helpers for sub_place_types and sub_places.

A sub-place is a semantically distinct location (a cafe, a shop, a hotel
lobby) that sits inside the radius of a single known place and would
otherwise be bundled into it. Each sub-place belongs to exactly one parent
known place; coordinates, radius and Wi-Fi SSID are optional and exist so a
later phase can infer which sub-places a visit touched.

Sub-places are never hard-deleted: archive_sub_place() sets archived_at so
future visit attributions keep resolving to a name (immutability convention).
Deleting the parent known place cascades, as place_visits does.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from database.base import BaseTable
from database.connection import get_conn, to_iso_str

DEFAULT_RADIUS_M = 30

# (category, name) — seeded once; free entry adds more via get_or_create_type().
DEFAULT_TYPES: list[tuple[str, str]] = [
    ("Food & drink", "cafe"), ("Food & drink", "restaurant"),
    ("Food & drink", "bar"), ("Food & drink", "bakery"),
    ("Retail", "shop"), ("Retail", "supermarket"),
    ("Retail", "market"), ("Retail", "pharmacy"),
    ("Stay & transport", "hotel"), ("Stay & transport", "hostel"),
    ("Stay & transport", "station"), ("Stay & transport", "airport terminal"),
    ("Culture & outdoors", "museum"), ("Culture & outdoors", "temple"),
    ("Culture & outdoors", "park"), ("Culture & outdoors", "viewpoint"),
]

_SELECT = """
    SELECT sp.id, sp.known_place_id, sp.name, sp.type_id, t.name AS type,
           sp.notes, sp.latitude, sp.longitude, sp.radius_m, sp.wifi_ssid,
           sp.created_at, sp.archived_at
    FROM sub_places sp
    LEFT JOIN sub_place_types t ON t.id = sp.type_id
"""


@dataclass
class SubPlaceRecord:
    known_place_id: int
    name: str
    type_id: Optional[int] = None
    notes: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    radius_m: int = DEFAULT_RADIUS_M
    wifi_ssid: Optional[str] = None


class SubPlacesTable(BaseTable[SubPlaceRecord]):

    def init(self) -> None:
        """Create sub_place_types and sub_places (and seed default types)."""
        with get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sub_place_types (
                    id       INTEGER PRIMARY KEY AUTOINCREMENT,
                    name     TEXT NOT NULL UNIQUE COLLATE NOCASE,
                    category TEXT
                );
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sub_places (
                    id             INTEGER PRIMARY KEY AUTOINCREMENT,
                    known_place_id INTEGER NOT NULL REFERENCES known_places(id) ON DELETE CASCADE,
                    name           TEXT NOT NULL,
                    type_id        INTEGER REFERENCES sub_place_types(id),
                    notes          TEXT,
                    latitude       REAL,
                    longitude      REAL,
                    radius_m       INTEGER NOT NULL DEFAULT 30,
                    wifi_ssid      TEXT,
                    created_at     TEXT NOT NULL,
                    archived_at    TEXT
                );
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_sub_places_known_place_id
                    ON sub_places(known_place_id);
            """)
            conn.executemany(
                "INSERT OR IGNORE INTO sub_place_types (category, name) VALUES (?, ?)",
                DEFAULT_TYPES,
            )

    # -- types ---------------------------------------------------------------

    def list_types(self) -> list[dict]:
        with get_conn(read_only=True) as conn:
            rows = conn.execute(
                "SELECT id, name, category FROM sub_place_types ORDER BY category IS NULL, category, name"
            ).fetchall()
        return [dict(r) for r in rows]

    def get_or_create_type(self, name: str) -> int:
        """Return the id of the type with this name (case-insensitive), creating it if new."""
        name = name.strip()
        with get_conn() as conn:
            conn.execute("INSERT OR IGNORE INTO sub_place_types (name) VALUES (?)", (name,))
            return conn.execute(
                "SELECT id FROM sub_place_types WHERE name = ?", (name,)
            ).fetchone()[0]

    # -- sub-places ----------------------------------------------------------

    def insert(self, record: SubPlaceRecord) -> int:
        """Insert a sub-place and return its id."""
        now = to_iso_str(datetime.now(timezone.utc))
        with get_conn() as conn:
            cursor = conn.execute("""
                INSERT INTO sub_places
                    (known_place_id, name, type_id, notes, latitude, longitude,
                     radius_m, wifi_ssid, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (record.known_place_id, record.name, record.type_id, record.notes,
                  record.latitude, record.longitude, record.radius_m,
                  record.wifi_ssid, now))
            return cursor.lastrowid

    def get(self, sub_place_id: int) -> Optional[dict]:
        with get_conn(read_only=True) as conn:
            row = conn.execute(_SELECT + " WHERE sp.id = ?", (sub_place_id,)).fetchone()
        return dict(row) if row else None

    def list_for_place(self, known_place_id: int, include_archived: bool = False) -> list[dict]:
        sql = _SELECT + " WHERE sp.known_place_id = ?"
        if not include_archived:
            sql += " AND sp.archived_at IS NULL"
        sql += " ORDER BY sp.name COLLATE NOCASE"
        with get_conn(read_only=True) as conn:
            rows = conn.execute(sql, (known_place_id,)).fetchall()
        return [dict(r) for r in rows]

    def update(self, sub_place_id: int, **fields) -> bool:
        """Update the given columns. Returns True if the row exists."""
        allowed = {"name", "type_id", "notes", "latitude", "longitude", "radius_m", "wifi_ssid"}
        cols = {k: v for k, v in fields.items() if k in allowed}
        if not cols:
            return self.get(sub_place_id) is not None
        set_sql = ", ".join(f"{k} = ?" for k in cols)
        with get_conn() as conn:
            cursor = conn.execute(
                f"UPDATE sub_places SET {set_sql} WHERE id = ?",
                (*cols.values(), sub_place_id),
            )
            return cursor.rowcount > 0

    def archive_sub_place(self, sub_place_id: int) -> bool:
        """Soft-delete. Returns True if an active sub-place was archived."""
        now = to_iso_str(datetime.now(timezone.utc))
        with get_conn() as conn:
            cursor = conn.execute(
                "UPDATE sub_places SET archived_at = ? WHERE id = ? AND archived_at IS NULL",
                (now, sub_place_id),
            )
            return cursor.rowcount > 0


table = SubPlacesTable()
