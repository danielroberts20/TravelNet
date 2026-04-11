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
                    origin_iata         TEXT NOT NULL, -- ISO 3166-3 airport code, e.g. "LHR"
                    destination_iata    TEXT NOT NULL, -- ISO 3166-3 airport code, e.g. "JFK"
                    origin_city         TEXT, -- e.g. "London"
                    destination_city    TEXT, -- e.g. "New York"
                    origin_country      TEXT, -- e.g. "United Kingdom"
                    destination_country TEXT, -- e.g. "United States"
                    departed_at         TEXT NOT NULL, -- ISO 8601 UTC timestamp of local departure time at the origin airport, e.g. "2024-06-01T14:30:00Z"
                    arrived_at          TEXT NOT NULL, -- ISO 8601 UTC timestamp of local arrival time at the destination airport, e.g. "2024-06-01T17:30:00Z"
                    duration_mins       INTEGER, -- total flight duration in minutes, e.g. 420
                    distance_km         REAL, -- great-circle distance between origin and destination in kilometers, e.g. 5567.0
                    airline             TEXT, -- IATA airline code or name, e.g. "BA" or "British Airways"
                    flight_number       TEXT, -- flight number, e.g. "BA117"
                    seat_class          TEXT, -- 'economy', 'premium_economy', 'business', or NULL if unknown
                    notes               TEXT, -- optional free-form notes about the flight
                    source              TEXT NOT NULL DEFAULT 'manual', -- 'manual' for user-entered, 'detected' for auto-detected from location gaps
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