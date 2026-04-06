"""
database/weather/table.py
~~~~~~~~~~~~~~~~~~~~~~~~~
Schema for the weather_hourly and weather_daily tables and the location_weather
view that joins them with location_unified.

Weather data is fetched retroactively from Open-Meteo's archive API and
co-located with GPS points so the dashboard can show conditions at each location.
"""

from database.connection import get_conn
import logging

logger = logging.getLogger(__name__)


def init() -> None:
    """Create weather tables, indexes, and the location_weather view if they do not exist."""
    with get_conn() as conn:
        try:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS weather_hourly (
                    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
                    fetched_at            TEXT NOT NULL,
                    timestamp             TEXT NOT NULL,
                    latitude              REAL NOT NULL,
                    longitude             REAL NOT NULL,
                    temperature_c         REAL,
                    apparent_temperature_c REAL,
                    precipitation_mm      REAL,
                    windspeed_kmh         REAL,
                    winddirection_deg     REAL,
                    uv_index              REAL,
                    cloudcover_pct        REAL,
                    is_day                INTEGER,
                    weathercode           INTEGER,
                    raw_json              TEXT,
                    UNIQUE(timestamp, latitude, longitude)
                );

                CREATE INDEX IF NOT EXISTS idx_weather_hourly_timestamp
                    ON weather_hourly(timestamp);

                CREATE INDEX IF NOT EXISTS idx_weather_hourly_lat_lon
                    ON weather_hourly(latitude, longitude);

                CREATE TABLE IF NOT EXISTS weather_daily (
                    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
                    fetched_at            TEXT NOT NULL,
                    date                  TEXT NOT NULL,
                    latitude              REAL NOT NULL,
                    longitude             REAL NOT NULL,
                    sunrise               TEXT,
                    sunset                TEXT,
                    precipitation_sum_mm  REAL,
                    precipitation_hours   REAL,
                    snowfall_sum_cm       REAL,
                    windspeed_max_kmh     REAL,
                    windgusts_max_kmh     REAL,
                    raw_json              TEXT,
                    UNIQUE(date, latitude, longitude)
                );

                CREATE INDEX IF NOT EXISTS idx_weather_daily_date
                    ON weather_daily(date);

                CREATE INDEX IF NOT EXISTS idx_weather_daily_lat_lon
                    ON weather_daily(latitude, longitude);
                
                CREATE VIEW IF NOT EXISTS location_weather AS
                SELECT
                    u.*,
                    p.country_code,
                    p.country,
                    p.city,
                    p.suburb,
                    -- hourly
                    h.temperature_c,
                    h.apparent_temperature_c,
                    h.precipitation_mm,
                    h.windspeed_kmh,
                    h.winddirection_deg,
                    h.uv_index,
                    h.cloudcover_pct,
                    h.is_day,
                    h.weathercode,
                    -- daily
                    d.sunrise,
                    d.sunset,
                    d.precipitation_sum_mm,
                    d.precipitation_hours,
                    d.snowfall_sum_cm,
                    d.windspeed_max_kmh,
                    d.windgusts_max_kmh
                FROM location_unified u
                LEFT JOIN places p
                    ON u.place_id = p.id
                LEFT JOIN weather_hourly h
                    ON  ROUND(u.latitude, 2) = h.latitude
                    AND ROUND(u.longitude, 2) = h.longitude
                    AND strftime('%Y-%m-%dT%H:00Z', u.timestamp) = h.timestamp
                LEFT JOIN weather_daily d
                    ON  ROUND(u.latitude, 2) = d.latitude
                    AND ROUND(u.longitude, 2) = d.longitude
                    AND DATE(u.timestamp) = d.date;
            """)
            logger.info("Migration complete: weather_hourly, weather_daily tables and views created.")
        except Exception:
            logger.exception("Migration failed.")
            raise