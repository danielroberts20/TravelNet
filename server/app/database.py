# database.py
import sqlite3
from pathlib import Path
from datetime import datetime
from typing import Optional, Tuple, List, Dict, get_type_hints

from telemetry_models import Log

# -----------------------------
# Database file setup
# -----------------------------
DB_DIR = Path("../data")
DB_DIR.mkdir(exist_ok=True, parents=True)
DB_FILE = DB_DIR / "travel.db"

# -----------------------------
# Connection helper
# -----------------------------
def get_conn() -> sqlite3.Connection:
    """Get a new SQLite connection."""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row  # so you can access columns by name
    return conn

# -----------------------------
# Initialization
# -----------------------------
def init_db():
    """Initialize all tables."""
    with get_conn() as conn:
        # Locations table
        conn.execute("""
        CREATE TABLE IF NOT EXISTS location_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,

            -- Core temporal data
            timestamp INTEGER NOT NULL,          -- Unix timestamp (seconds)
            timezone TEXT,                       -- e.g. "+0000, -0300"

            -- Geographic identifiers
            country_code TEXT,                   -- e.g. "GBR"
            latitude REAL NOT NULL,
            longitude REAL NOT NULL,
            altitude REAL,                       -- meters
            activity TEXT,     

            -- Device data      
            device TEXT,                         -- e.g. "Mac", "iPhone"
            is_locked BOOLEAN,
            battery INTEGER,
            is_charging BOOLEAN,
            is_connected_charger BOOLEAN,
                     
            -- Network data      
            BSSID TEXT,
            RSSI INTEGER,

            created_at INTEGER DEFAULT (strftime('%s','now'))
        );""")

        conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_location_timestamp
            ON location_history(timestamp);
        """)

        conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_location_country
            ON location_history(country_code);
        """)

        conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_location_device
            ON location_history(device);
        """)

        conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_location_time_country
            ON location_history(timestamp, country_code);
        """)

        # Cellular state table
        conn.execute("""
        CREATE TABLE IF NOT EXISTS cellular_state (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            location_id INTEGER NOT NULL,
            provider_name TEXT,
            radio TEXT,
            code TEXT,
            is_roaming BOOLEAN,
            FOREIGN KEY(location_id) REFERENCES location(id) ON DELETE CASCADE
        );""")

        # FX rates table (optional)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS fx_rates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,                -- YYYY-MM-DD
            source_currency TEXT NOT NULL,
            target_currency TEXT NOT NULL,
            rate REAL NOT NULL,
            timestamp INTEGER NOT NULL,        -- Time when fetched
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)

# Initialize DB on import
init_db()

# -----------------------------
# Location helpers
# -----------------------------
def insert_location(log: Log) -> int:
    """Insert a location into the DB. Returns the inserted row id."""
    formatted = datetime.fromtimestamp(log.timestamp).strftime("%a, %d %b %Y %H:%M:%S")
    with get_conn() as conn:
        cursor = conn.execute(
            "INSERT INTO location_history (timestamp, timezone, country_code, latitude, longitude, altitude, activity, device, is_locked, battery, is_charging, is_connected_charger, BSSID, RSSI) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (log.timestamp,
             log.timezone,
             log.country_code,
             log.latitude,
             log.longitude,
             log.altitude,
             log.activity,
             log.device,
             log.is_locked,
             log.battery,
             log.is_charging,
             log.is_connected_charger,
             log.BSSID,
             log.RSSI)
        )
        return cursor.lastrowid

def insert_log(log: Log):
    location_id = insert_location(log)
    with get_conn() as conn:
        for cs in log.cellular_states:
            conn.execute("""
                INSERT INTO cellular_state (location_id, provider_name, radio, code, is_roaming) VALUES (?, ?, ?, ?, ?)""", 
                (location_id,
                 cs.provider_name,
                 cs.radio,
                 cs.code,
                 cs.is_roaming))

def fetch_locations(limit: int = 100) -> List[Dict]:
    """Fetch last N locations."""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM location_history ORDER BY timestamp DESC LIMIT ?",
            (limit,)
        ).fetchall()
        return [dict(row) for row in rows]

# -----------------------------
# FX helpers
# -----------------------------
def insert_fx_rate(date: str, source_currency: str, target_currency: str, rate: float, ts: Optional[int] = None) -> int:
    """Insert FX rate. ts defaults to now if not provided."""
    if ts is None:
        ts = int(datetime.now().timestamp())
    with get_conn() as conn:
        cursor = conn.execute(
            "INSERT INTO fx_rates (date, source_currency, target_currency, rate, timestamp) VALUES (?, ?, ?, ?, ?)",
            (date, source_currency, target_currency, rate, ts)
        )
        return cursor.lastrowid

def fetch_fx_rates(limit: int = 100) -> List[Dict]:
    """Fetch last N FX rates."""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM fx_rates ORDER BY timestamp DESC LIMIT ?",
            (limit,)
        ).fetchall()
        return [dict(row) for row in rows]

# -----------------------------
# Helper to fetch latest FX rate for a currency pair
# -----------------------------
def get_latest_fx(source_currency: str, target_currency: str) -> Optional[Dict]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM fx_rates WHERE source_currency=? AND target_currency=? ORDER BY timestamp DESC LIMIT 1",
            (source_currency, target_currency)
        ).fetchone()
        return dict(row) if row else None
    
