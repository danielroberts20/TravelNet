from database.util import get_conn

def init():
    with get_conn() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS health_data (
            id INTEGER PRIMARY KEY,
            timestamp INTEGER NOT NULL,      -- Unix seconds
            metric TEXT NOT NULL,            -- e.g., "Heart Rate"
            value_json TEXT,                 -- JSON of metric sub-values
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
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

def insert_health_entry(timestamp: int, metric: str, value_json: str, sources: list[str]):
    with get_conn() as conn:
        cursor = conn.execute("""
        INSERT OR IGNORE INTO health_data (timestamp, metric, value_json)
        VALUES (?, ?, ?);
        """, (timestamp, metric, value_json))
        health_id = cursor.lastrowid

        if sources != ['']:
            for source in sources:
                conn.execute("""
                INSERT OR IGNORE INTO health_sources (health_id, source)
                VALUES (?, ?);
                """, (health_id, source))