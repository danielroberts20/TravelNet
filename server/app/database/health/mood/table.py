import json

from database.util import get_conn, to_iso_str


def init() -> None:
    with get_conn as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS state_of_mind (
                id              TEXT PRIMARY KEY,
                kind            TEXT NOT NULL,
                start_time      TEXT NOT NULL,
                end_time        TEXT NOT NULL,
                valence         REAL NOT NULL,
                classification  TEXT NOT NULL,
                labels          TEXT NOT NULL DEFAULT '[]',
                associations    TEXT NOT NULL DEFAULT '[]',
                uploaded_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
            );
            """)
        
        conn.execute("""CREATE INDEX IF NOT EXISTS idx_state_of_mind_start_time ON state_of_mind (start_time);""")
        conn.execute("""CREATE INDEX IF NOT EXISTS idx_state_of_mind_valence ON state_of_mind (valence);""")
        conn.execute("""CREATE INDEX IF NOT EXISTS idx_state_of_mind_classification ON state_of_mind (classification);""")


def insert_state_of_mind(entries: list[dict]) -> int:
    with get_conn() as conn:
        """Insert state of mind entries, ignoring duplicates. Returns inserted count."""
        rows = []
        for e in entries:
            rows.append((
                e["id"],
                e.get("kind", "momentary_emotion"),
                to_iso_str(e["start"]),
                to_iso_str(e["end"]),
                e["valence"],
                e["valenceClassification"],
                json.dumps(sorted(e.get("labels", []))),
                json.dumps(sorted(e.get("associations", []))),
            ))

        cursor = conn.executemany(
            """
            INSERT OR IGNORE INTO state_of_mind
                (id, kind, start_time, end_time, valence, classification, labels, associations)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
    return cursor.rowcount