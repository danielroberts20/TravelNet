from database.util import get_conn

def init():
    with get_conn() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS health_data (
            id INTEGER PRIMARY KEY,
            timestamp INTEGER NOT NULL,        -- Unix seconds
            device TEXT NOT NULL,              -- "Apple Watch" or "Xiaomi Band"
            heart_rate INTEGER,                -- bpm
            hrv REAL,                          -- optional, milliseconds
            steps INTEGER,                      -- steps since last log
            activity TEXT,                      -- e.g., "Walking", "Running", "Stationary"
            sleep_stage TEXT,                   -- e.g., "Awake", "Light", "Deep", "REM"
            spO2 REAL,                          -- optional, %
            stress REAL,                        -- optional, arbitrary units from band
            temperature REAL,                   -- optional skin temperature
            source TEXT,                        -- optional, name of app or raw source
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(timestamp, device)           -- ensures no duplicate rows per device
        );
        """)

        # Indexes for performance
        conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_health_timestamp
            ON health_data(timestamp);
        """)

        conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_health_device_timestamp
            ON health_data(device, timestamp);
        """)