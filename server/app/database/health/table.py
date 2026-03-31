"""
database/health/table.py
~~~~~~~~~~~~~~~~~~~~~~~~
Schema and insert helpers for health_data and health_sources tables.

health_data stores one aggregated row per (timestamp bucket, metric).
health_sources stores the Apple device(s) that contributed to each row —
a separate child table so a single health entry can reference multiple sources
without duplicating the metric data.
"""

from database.connection import get_conn, to_iso_str


def init() -> None:
    """Create the health_data and health_sources tables and their indexes if they do not exist."""
    with get_conn() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS health_data (
            id INTEGER PRIMARY KEY,
            timestamp TEXT NOT NULL,
            metric TEXT NOT NULL,            -- e.g., "Heart Rate"
            value_json TEXT,                 -- JSON of metric sub-values
            created_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
            UNIQUE(timestamp, metric)
        );
        """)

        conn.execute("""
        CREATE TABLE IF NOT EXISTS health_sources (
            health_id INTEGER NOT NULL,
            source TEXT NOT NULL,           -- e.g., "Apple Watch", "iPhone"
            PRIMARY KEY(health_id, source),
            FOREIGN KEY (health_id) REFERENCES health_data(id)
        );""")

        # Indexes for performance
        conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_health_timestamp
            ON health_data(timestamp);
        """)


def insert_health_entry(timestamp: int, metric: str, value_json: str, sources: list[str]) -> None:
    """Insert a health data row and its source records.

    Uses INSERT OR IGNORE on health_data (UNIQUE timestamp+metric) so
    re-processing the same upload is idempotent.  Source rows are also inserted
    with INSERT OR IGNORE against their composite primary key.
    """
    new_ts = to_iso_str(timestamp)
    with get_conn() as conn:
        cursor = conn.execute("""
            INSERT OR IGNORE INTO health_data (timestamp, metric, value_json)
            VALUES (?, ?, ?);
        """, (new_ts, metric, value_json))

        health_id = cursor.lastrowid
        if not health_id:
            # Row already existed — fetch its id
            row = conn.execute("""
                SELECT id FROM health_data WHERE timestamp = ? AND metric = ?
            """, (new_ts, metric)).fetchone()
            if row is None:
                return
            health_id = row[0]

        if sources:
            for source in sources:
                conn.execute("""
                    INSERT OR IGNORE INTO health_sources (health_id, source)
                    VALUES (?, ?);
                """, (health_id, source))
