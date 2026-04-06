"""
database/ml/table.py
~~~~~~~~~~~~~~~~~~~~
Schema for ML output tables.

These are stub tables created at startup so foreign keys exist from day one
and ML work can begin without schema changes. They will be populated post-
departure once the ML pipeline is running.
"""

from database.connection import get_conn


def init() -> None:
    """Create ML output tables and their indexes if they do not exist."""
    with get_conn() as conn:
        # HDBSCAN/DBSCAN location clusters
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

        # Per-point cluster assignment
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ml_location_cluster_members (
                overland_id INTEGER NOT NULL REFERENCES location_overland(id) ON DELETE CASCADE,
                cluster_id  INTEGER NOT NULL REFERENCES ml_location_clusters(id),
                PRIMARY KEY (overland_id, cluster_id)
            );
        """)

        # HMM activity/segment labels
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

        # Anomaly flags
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
