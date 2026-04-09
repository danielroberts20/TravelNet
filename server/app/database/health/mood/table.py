"""
database/health/mood/table.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Schema and insert helpers for the state_of_mind, mood_labels, and
mood_associations tables.

state_of_mind stores one row per HealthKit check-in. Labels and associations
are normalised into child tables rather than stored as JSON blobs, so they
can be queried and indexed independently.

MoodRecord represents one state_of_mind entry including its labels and
associations. insert() handles all three tables in a single connection.
"""

from dataclasses import dataclass, field

from database.base import BaseTable
from database.connection import get_conn, to_iso_str


@dataclass
class MoodRecord:
    id: str
    kind: str
    start: int    # Unix timestamp
    end: int      # Unix timestamp
    valence: float
    valence_classification: str
    labels: list[str] = field(default_factory=list)
    associations: list[str] = field(default_factory=list)


class MoodTable(BaseTable[MoodRecord]):

    def init(self) -> None:
        """Create the state_of_mind, mood_labels, and mood_associations tables."""
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

    def insert(self, record: MoodRecord) -> None:
        """Insert a single state_of_mind entry with its labels and associations.

        Idempotent on id. Labels and associations are inserted with INSERT OR IGNORE
        against their composite primary keys.
        """
        with get_conn() as conn:
            cursor = conn.execute("""
                INSERT OR IGNORE INTO state_of_mind
                    (id, kind, start_ts, end_ts, valence, classification)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                record.id,
                record.kind,
                to_iso_str(record.start),
                to_iso_str(record.end),
                record.valence,
                record.valence_classification,
            ))

            if cursor.rowcount == 0:
                return

            for label in sorted(set(record.labels)):
                conn.execute(
                    "INSERT OR IGNORE INTO mood_labels (som_id, label) VALUES (?, ?)",
                    (record.id, label),
                )

            for association in sorted(set(record.associations)):
                conn.execute(
                    "INSERT OR IGNORE INTO mood_associations (som_id, association) VALUES (?, ?)",
                    (record.id, association),
                )

            conn.commit()

    def batch_insert(self, entries: list[dict]) -> int:
        """Insert multiple state_of_mind entries from the raw upload dict format.

        Accepts the original list[dict] format from the health upload router for
        compatibility. Returns count of newly inserted rows.
        """
        inserted = 0
        for e in entries:
            self.insert(MoodRecord(
                id=e["id"],
                kind=e.get("kind", "momentaryEmotion"),
                start=e["start"],
                end=e["end"],
                valence=e["valence"],
                valence_classification=e["valenceClassification"],
                labels=e.get("labels", []),
                associations=e.get("associations", []),
            ))
            inserted += 1
        return inserted


table = MoodTable()
