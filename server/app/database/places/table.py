"""
database/places/table.py
~~~~~~~~~~~~~~~~~~~~~~~~
Schema for the places table — a central geographic reference used by all
other tables that record "where" (location, health, transactions, etc.).

Points are snapped to a ~111m grid (3 d.p.) to allow fuzzy joins without
triggering duplicate geocode lookups for nearby fixes.
"""

from database.connection import get_conn


def init() -> None:
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

        conn.execute("""
        CREATE TABLE IF NOT EXISTS place_visits (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            place_id        INTEGER NOT NULL REFERENCES known_places(id),
            arrived_at      TEXT NOT NULL,     -- ISO 8601 UTC
            departed_at     TEXT,              -- NULL until departure detected
            duration_mins   INTEGER            -- NULL until departure detected
        );""")

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_place_visits_arrived_at
                ON place_visits(arrived_at);
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_place_visits_departed_at
                ON place_visits(departed_at);
        """)
