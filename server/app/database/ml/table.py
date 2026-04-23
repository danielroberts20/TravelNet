"""
database/ml/table.py
~~~~~~~~~~~~~~~~~~~~
Schema for ML output tables.

These tables are created at startup so foreign keys exist from day one.
They will be populated post-departure once the ML pipeline is running.
insert() is a no-op placeholder — ML outputs are written by the compute
pipeline, not through this interface.
"""

from dataclasses import dataclass

from database.base import BaseTable
from database.connection import get_conn


@dataclass
class MlRecord:
    """Placeholder record type — ML insert pipeline is TBD post-departure."""
    pass


class MlTable(BaseTable[MlRecord]):

    def init(self) -> None:
        """Create ML output tables and their indexes if they do not exist."""
        with get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ml_location_clusters (
                    id           INTEGER PRIMARY KEY,
                    label        INTEGER NOT NULL,
                    centroid_lat REAL,
                    centroid_lon REAL,
                    place_id     INTEGER REFERENCES places(id),
                    description  TEXT,
                    created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
                );
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS ml_location_cluster_members (
                    overland_id INTEGER NOT NULL REFERENCES location_overland(id) ON DELETE CASCADE,
                    cluster_id  INTEGER NOT NULL REFERENCES ml_location_clusters(id),
                    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
                    PRIMARY KEY (overland_id, cluster_id)
                );
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS ml_segments (
                    id            INTEGER PRIMARY KEY,
                    start_ts      TEXT NOT NULL,
                    end_ts        TEXT NOT NULL,
                    label         TEXT NOT NULL,
                    confidence    REAL,
                    model_version TEXT,
                    created_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
                );
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_ml_seg_start
                    ON ml_segments(start_ts);
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS ml_anomalies (
                    id           INTEGER PRIMARY KEY,
                    detected_at  TEXT NOT NULL,
                    domain       TEXT NOT NULL,
                    anomaly_type TEXT NOT NULL,
                    severity     REAL,
                    context_json TEXT,
                    explained    INTEGER DEFAULT 0,
                    place_id     INTEGER REFERENCES places(id),
                    created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
                );
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_anomaly_domain
                    ON ml_anomalies(domain);
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_anomaly_detected
                    ON ml_anomalies(detected_at);
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_anomaly_unexplained
                    ON ml_anomalies(explained) WHERE explained = 0;
            """)

    def insert(self, record: MlRecord) -> None:
        """No-op placeholder. ML outputs are written by the compute pipeline."""
        pass


table = MlTable()
