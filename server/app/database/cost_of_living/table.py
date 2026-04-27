import math
from dataclasses import dataclass
from typing import Optional
 
from util import haversine_km
from database.base import BaseTable
from database.connection import get_conn
 
 
@dataclass
class CostOfLivingRecord:
    country_code: str
    country: str
    city: str
    center_lat: Optional[float]
    center_lon: Optional[float]
    col_index: Optional[float]
    rent_index: Optional[float]
    col_plus_rent: Optional[float]
    groceries_index: Optional[float]
    restaurant_index: Optional[float]
    local_currency: Optional[str]
    source: str
    reference_year: int
    is_estimated: bool | int
    notes: Optional[str]
 
 
class CostOfLivingTable(BaseTable[CostOfLivingRecord]):
 
    def init(self) -> None:
        with get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS cost_of_living (
                    id               INTEGER PRIMARY KEY AUTOINCREMENT,
                    country_code     TEXT NOT NULL,       -- ISO 3166-1 alpha-2, e.g. "AU"
                    country          TEXT NOT NULL,
                    city             TEXT NOT NULL,       -- '' (empty string) for country-level entries
                    center_lat       REAL,
                    center_lon       REAL,
                    col_index        REAL,                -- Numbeo CoL Index, NYC = 100, excludes rent
                    rent_index       REAL,
                    col_plus_rent    REAL,                -- CoL + Rent combined index
                    groceries_index  REAL,
                    restaurant_index REAL,
                    local_currency   TEXT,                -- ISO 4217, e.g. "AUD"
                    source           TEXT NOT NULL,       -- e.g. "Numbeo 2025"
                    reference_year   INTEGER NOT NULL,
                    is_estimated     INTEGER NOT NULL DEFAULT 0,  -- 1 = no direct data, proxied
                    notes            TEXT,                -- reason for proxy/estimate
                    created_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
                    UNIQUE(country_code, city)
                );
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_col_country 
                    ON cost_of_living(country_code);
            """)
 
    def insert(self, record: CostOfLivingRecord) -> bool:
        """Insert a flight record. Returns True if inserted, False if ignored."""
        with get_conn() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO cost_of_living (
                    country_code, country, city, center_lat,
                    center_lon, col_index, rent_index, col_plus_rent,
                    groceries_index, restaurant_index,
                    local_currency, source, reference_year,
                    is_estimated, notes
                ) VALUES (
                    ?, ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?,
                    ?, ?, ?,
                    ?, ?
                )
            """, (
                record.country_code, record.country,
                record.city, record.center_lat, record.center_lon,
                record.col_index, record.rent_index, record.col_plus_rent,
                record.groceries_index, record.restaurant_index,
                record.local_currency, record.source,
                record.reference_year, record.is_estimated, record.notes
            ))
            return conn.execute("SELECT changes()").fetchone()[0] > 0

table = CostOfLivingTable()