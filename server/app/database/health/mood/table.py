"""
database/health/mood/table.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Schema for state_of_mind, mood_labels, and mood_associations tables.

state_of_mind stores one row per HealthKit check-in. Labels and associations
are normalised into child tables (mood_labels / mood_associations) rather than
stored as JSON blobs, so they can be queried and indexed independently.
"""

from database.connection import get_conn, to_iso_str


def init() -> None:
    with get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS state_of_mind (
                id              TEXT PRIMARY KEY,
                kind            TEXT NOT NULL,
                start_ts        TEXT NOT NULL,
                end_ts          TEXT NOT NULL,
                valence         REAL NOT NULL CHECK(valence BETWEEN -1.0 AND 1.0),
                classification  TEXT NOT NULL,
                place_id        INTEGER REFERENCES places(id),
                uploaded_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
            );
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_som_start
                ON state_of_mind(start_ts);
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_som_valence
                ON state_of_mind(valence);
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_som_classification
                ON state_of_mind(classification);
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS mood_labels (
                id      INTEGER PRIMARY KEY,
                som_id  TEXT NOT NULL REFERENCES state_of_mind(id) ON DELETE CASCADE,
                label   TEXT NOT NULL,
                UNIQUE(som_id, label)
            );
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS mood_associations (
                id          INTEGER PRIMARY KEY,
                som_id      TEXT NOT NULL REFERENCES state_of_mind(id) ON DELETE CASCADE,
                association TEXT NOT NULL,
                UNIQUE(som_id, association)
            );
        """)


def insert_state_of_mind(entries: list[dict]) -> int:
    """Insert state of mind entries, ignoring duplicates. Returns inserted count.

    Each entry dict must have: id, kind, start (timestamp), end (timestamp),
    valence, valenceClassification, and optionally labels and associations lists.
    Labels and associations are written to their own normalised tables.
    """
    inserted = 0
    with get_conn() as conn:
        for e in entries:
            cursor = conn.execute("""
                INSERT OR IGNORE INTO state_of_mind
                    (id, kind, start_ts, end_ts, valence, classification)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                e["id"],
                e.get("kind", "momentaryEmotion"),
                to_iso_str(e["start"]),
                to_iso_str(e["end"]),
                e["valence"],
                e["valenceClassification"],
            ))

            if cursor.rowcount == 0:
                continue

            inserted += 1
            som_id = e["id"]

            for label in sorted(set(e.get("labels", []))):
                conn.execute("""
                    INSERT OR IGNORE INTO mood_labels (som_id, label) VALUES (?, ?)
                """, (som_id, label))

            for association in sorted(set(e.get("associations", []))):
                conn.execute("""
                    INSERT OR IGNORE INTO mood_associations (som_id, association) VALUES (?, ?)
                """, (som_id, association))

        conn.commit()
    return inserted
