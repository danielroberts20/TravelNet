"""
database/flights/table.py
~~~~~~~~~~~~~~~~~~~~~~~~~
Schema and helpers for the flights table.
 
Populated via:
  - POST /upload/flight  (manual entry from Dashboard or Shortcut)
  - detect_flights.py    (auto-detected from large location gaps, source='detected')
 
Detected rows have source='detected' and should be confirmed/edited via the
Dashboard before being treated as authoritative.
"""
 
import math
from dataclasses import dataclass
from typing import Optional
 
from util import haversine_km
from database.base import BaseTable
from database.connection import get_conn
 
 
@dataclass
class FlightRecord:
    origin_iata: str
    destination_iata: str
    departed_at: str            # ISO 8601 UTC
    arrived_at: str             # ISO 8601 UTC
    origin_city: Optional[str] = None
    destination_city: Optional[str] = None
    origin_country: Optional[str] = None
    destination_country: Optional[str] = None
    duration_mins: Optional[int] = None
    distance_km: Optional[float] = None
    airline: Optional[str] = None
    flight_number: Optional[str] = None
    seat_class: Optional[str] = None    # 'economy', 'premium_economy', 'business'
    notes: Optional[str] = None
    source: str = "manual"              # 'manual' or 'detected'
 
 
class FlightsTable(BaseTable[FlightRecord]):
 
    def init(self) -> None:
        with get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS flights (
                    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                    origin_iata         TEXT NOT NULL,
                    destination_iata    TEXT NOT NULL,
                    origin_city         TEXT,
                    destination_city    TEXT,
                    origin_country      TEXT,
                    destination_country TEXT,
                    departed_at         TEXT NOT NULL,
                    arrived_at          TEXT NOT NULL,
                    duration_mins       INTEGER,
                    distance_km         REAL,
                    airline             TEXT,
                    flight_number       TEXT,
                    seat_class          TEXT,
                    notes               TEXT,
                    source              TEXT NOT NULL DEFAULT 'manual',
                    created_at          TEXT NOT NULL
                        DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
                    UNIQUE(origin_iata, destination_iata, departed_at)
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_flights_departed
                ON flights(departed_at)
            """)
 
    def insert(self, record: FlightRecord) -> bool:
        """Insert a flight record. Returns True if inserted, False if ignored."""
        with get_conn() as conn:
            conn.execute("""
                INSERT OR IGNORE INTO flights (
                    origin_iata, destination_iata,
                    origin_city, destination_city,
                    origin_country, destination_country,
                    departed_at, arrived_at,
                    duration_mins, distance_km,
                    airline, flight_number, seat_class,
                    notes, source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                record.origin_iata, record.destination_iata,
                record.origin_city, record.destination_city,
                record.origin_country, record.destination_country,
                record.departed_at, record.arrived_at,
                record.duration_mins, record.distance_km,
                record.airline, record.flight_number, record.seat_class,
                record.notes, record.source,
            ))
            return conn.execute("SELECT changes()").fetchone()[0] > 0

table = FlightsTable()