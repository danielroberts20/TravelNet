"""
database/weather/table.py
~~~~~~~~~~~~~~~~~~~~~~~~~
Schema and insert helpers for the weather_hourly and weather_daily tables,
and the place_weather view that joins them with places.

Weather data is fetched retroactively from Open-Meteo's archive API. The
place_weather view links each place to all weather rows for its grid cell
(lat/lon rounded to COORD_PRECISION decimal places), enabling queries like
"find places where it was snowing" that can then be joined to transactions,
health data, or any other table that carries a place_id.

insert_hourly_batch() and insert_daily_batch() accept the raw Open-Meteo API
response dicts and handle unpacking, alignment, and idempotent insertion in a
single connection. These are called by scheduled_tasks/get_weather.py.
"""

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from database.base import BaseTable
from database.connection import get_conn, to_iso_str

logger = logging.getLogger(__name__)


@dataclass
class WeatherHourlyRecord:
    timestamp: str
    latitude: float
    longitude: float
    fetched_at: str
    temperature_c: float | None = None
    apparent_temperature_c: float | None = None
    precipitation_mm: float | None = None
    windspeed_kmh: float | None = None
    winddirection_deg: float | None = None
    uv_index: float | None = None
    cloudcover_pct: float | None = None
    is_day: int | None = None
    weathercode: int | None = None
    raw_json: str | None = None


@dataclass
class WeatherDailyRecord:
    date: str
    latitude: float
    longitude: float
    fetched_at: str
    sunrise: str | None = None
    sunset: str | None = None
    precipitation_sum_mm: float | None = None
    precipitation_hours: float | None = None
    snowfall_sum_cm: float | None = None
    windspeed_max_kmh: float | None = None
    windgusts_max_kmh: float | None = None
    raw_json: str | None = None


class WeatherTable(BaseTable[WeatherHourlyRecord]):
    """Manages both weather_hourly and weather_daily tables.

    insert() handles single hourly records. Use insert_daily() for daily records.
    Use the batch variants for bulk ingest from the Open-Meteo API.
    """

    def init(self) -> None:
        """Create weather tables, indexes, and the place_weather view."""
        with get_conn() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS weather_hourly (
                    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
                    fetched_at              TEXT NOT NULL,
                    timestamp               TEXT NOT NULL,
                    latitude                REAL NOT NULL,
                    longitude               REAL NOT NULL,
                    temperature_c           REAL,
                    apparent_temperature_c  REAL,
                    precipitation_mm        REAL,
                    windspeed_kmh           REAL,
                    winddirection_deg       REAL,
                    uv_index                REAL,
                    cloudcover_pct          REAL,
                    is_day                  INTEGER,
                    weathercode             INTEGER,
                    raw_json                TEXT,
                    UNIQUE(timestamp, latitude, longitude)
                );

                CREATE INDEX IF NOT EXISTS idx_weather_hourly_timestamp
                    ON weather_hourly(timestamp);

                CREATE INDEX IF NOT EXISTS idx_weather_hourly_lat_lon
                    ON weather_hourly(latitude, longitude);

                CREATE INDEX IF NOT EXISTS idx_weather_hourly_lat_lon_ts
                    ON weather_hourly(latitude, longitude, timestamp);

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

                CREATE INDEX IF NOT EXISTS idx_weather_daily_lat_lon_date
                    ON weather_daily(latitude, longitude, date);

                CREATE VIEW IF NOT EXISTS place_weather AS
                SELECT
                    p.id             AS place_id,
                    p.lat_snap,
                    p.lon_snap,
                    p.country_code,
                    p.country,
                    p.region,
                    p.city,
                    p.suburb,
                    p.road,
                    p.display_name,
                    p.timezone,
                    h.timestamp,
                    h.fetched_at,
                    h.temperature_c,
                    h.apparent_temperature_c,
                    h.precipitation_mm,
                    h.windspeed_kmh,
                    h.winddirection_deg,
                    h.uv_index,
                    h.cloudcover_pct,
                    h.is_day,
                    h.weathercode,
                    d.date,
                    d.sunrise,
                    d.sunset,
                    d.precipitation_sum_mm,
                    d.precipitation_hours,
                    d.snowfall_sum_cm,
                    d.windspeed_max_kmh,
                    d.windgusts_max_kmh
                FROM places p
                LEFT JOIN weather_hourly h
                    ON  ROUND(p.lat_snap, 1) = h.latitude
                    AND ROUND(p.lon_snap, 1) = h.longitude
                LEFT JOIN weather_daily d
                    ON  ROUND(p.lat_snap, 1) = d.latitude
                    AND ROUND(p.lon_snap, 1) = d.longitude
                    AND DATE(h.timestamp) = d.date;
            """)

    def insert(self, record: WeatherHourlyRecord) -> None:
        """Insert a single hourly weather row. Idempotent on (timestamp, lat, lon)."""
        with get_conn() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO weather_hourly
                    (fetched_at, timestamp, latitude, longitude,
                     temperature_c, apparent_temperature_c, precipitation_mm,
                     windspeed_kmh, winddirection_deg, uv_index, cloudcover_pct,
                     is_day, weathercode, raw_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.fetched_at, record.timestamp, record.latitude, record.longitude,
                    record.temperature_c, record.apparent_temperature_c, record.precipitation_mm,
                    record.windspeed_kmh, record.winddirection_deg, record.uv_index,
                    record.cloudcover_pct, record.is_day, record.weathercode, record.raw_json,
                ),
            )

    def insert_daily(self, record: WeatherDailyRecord) -> None:
        """Insert a single daily weather row. Idempotent on (date, lat, lon)."""
        with get_conn() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO weather_daily
                    (fetched_at, date, latitude, longitude,
                     sunrise, sunset, precipitation_sum_mm, precipitation_hours,
                     snowfall_sum_cm, windspeed_max_kmh, windgusts_max_kmh, raw_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.fetched_at, record.date, record.latitude, record.longitude,
                    record.sunrise, record.sunset, record.precipitation_sum_mm,
                    record.precipitation_hours, record.snowfall_sum_cm,
                    record.windspeed_max_kmh, record.windgusts_max_kmh, record.raw_json,
                ),
            )

    def insert_hourly_batch(self, data: dict, lat: float, lon: float) -> int:
        """Insert hourly rows from an Open-Meteo API response dict.

        Unpacks the parallel arrays in data['hourly'] and inserts each row in a
        single connection. Returns count of newly inserted rows.
        """
        hourly = data.get("hourly", {})
        times                 = hourly.get("time", [])
        temperatures          = hourly.get("temperature_2m", [])
        apparent_temperatures = hourly.get("apparent_temperature", [])
        precipitations        = hourly.get("precipitation", [])
        windspeeds            = hourly.get("windspeed_10m", [])
        winddirections        = hourly.get("winddirection_10m", [])
        weathercodes          = hourly.get("weathercode", [])
        uv_indices            = hourly.get("uv_index", [])
        cloudcovers           = hourly.get("cloudcover", [])
        is_days               = hourly.get("is_day", [])

        fetched_at = to_iso_str(datetime.now(timezone.utc))
        inserted = 0

        with get_conn() as conn:
            for i, ts in enumerate(times):
                raw = {
                    "temperature_2m":       temperatures[i]          if i < len(temperatures)          else None,
                    "apparent_temperature": apparent_temperatures[i]  if i < len(apparent_temperatures) else None,
                    "precipitation":        precipitations[i]         if i < len(precipitations)        else None,
                    "windspeed_10m":        windspeeds[i]             if i < len(windspeeds)            else None,
                    "winddirection_10m":    winddirections[i]         if i < len(winddirections)        else None,
                    "weathercode":          weathercodes[i]           if i < len(weathercodes)          else None,
                    "uv_index":             uv_indices[i]             if i < len(uv_indices)            else None,
                    "cloudcover":           cloudcovers[i]            if i < len(cloudcovers)           else None,
                    "is_day":               is_days[i]                if i < len(is_days)               else None,
                }
                cursor = conn.execute(
                    """
                    INSERT OR IGNORE INTO weather_hourly
                        (fetched_at, timestamp, latitude, longitude,
                         temperature_c, apparent_temperature_c, precipitation_mm,
                         windspeed_kmh, winddirection_deg, uv_index, cloudcover_pct,
                         is_day, weathercode, raw_json)
                    VALUES
                        (:fetched_at, :timestamp, :lat, :lon,
                         :temp, :apparent_temp, :precip,
                         :wind, :winddir, :uv, :cloud,
                         :is_day, :code, :raw)
                    """,
                    {
                        "fetched_at":    fetched_at,
                        "timestamp":     to_iso_str(ts),
                        "lat":           lat,
                        "lon":           lon,
                        "temp":          raw["temperature_2m"],
                        "apparent_temp": raw["apparent_temperature"],
                        "precip":        raw["precipitation"],
                        "wind":          raw["windspeed_10m"],
                        "winddir":       raw["winddirection_10m"],
                        "uv":            raw["uv_index"],
                        "cloud":         raw["cloudcover"],
                        "is_day":        raw["is_day"],
                        "code":          raw["weathercode"],
                        "raw":           json.dumps(raw),
                    },
                )
                inserted += cursor.rowcount

        return inserted

    def insert_daily_batch(self, data: dict, lat: float, lon: float) -> int:
        """Insert daily rows from an Open-Meteo API response dict.

        Returns count of newly inserted rows.
        """
        daily = data.get("daily", {})
        dates           = daily.get("time", [])
        sunrises        = daily.get("sunrise", [])
        sunsets         = daily.get("sunset", [])
        precip_sums     = daily.get("precipitation_sum", [])
        precip_hours    = daily.get("precipitation_hours", [])
        snowfall_sums   = daily.get("snowfall_sum", [])
        windspeed_maxes = daily.get("wind_speed_10m_max", [])
        windgust_maxes  = daily.get("wind_gusts_10m_max", [])

        fetched_at = to_iso_str(datetime.now(timezone.utc))
        inserted = 0

        with get_conn() as conn:
            for i, d in enumerate(dates):
                raw = {
                    "sunrise":             sunrises[i]        if i < len(sunrises)        else None,
                    "sunset":              sunsets[i]         if i < len(sunsets)         else None,
                    "precipitation_sum":   precip_sums[i]     if i < len(precip_sums)     else None,
                    "precipitation_hours": precip_hours[i]    if i < len(precip_hours)    else None,
                    "snowfall_sum":        snowfall_sums[i]   if i < len(snowfall_sums)   else None,
                    "wind_speed_10m_max":  windspeed_maxes[i] if i < len(windspeed_maxes) else None,
                    "wind_gusts_10m_max":  windgust_maxes[i]  if i < len(windgust_maxes)  else None,
                }
                cursor = conn.execute(
                    """
                    INSERT OR IGNORE INTO weather_daily
                        (fetched_at, date, latitude, longitude,
                         sunrise, sunset, precipitation_sum_mm, precipitation_hours,
                         snowfall_sum_cm, windspeed_max_kmh, windgusts_max_kmh, raw_json)
                    VALUES
                        (:fetched_at, :date, :lat, :lon,
                         :sunrise, :sunset, :precip_sum, :precip_hours,
                         :snowfall, :windspeed_max, :windgusts_max, :raw)
                    """,
                    {
                        "fetched_at":     fetched_at,
                        "date":           d,
                        "lat":            lat,
                        "lon":            lon,
                        "sunrise":        raw["sunrise"],
                        "sunset":         raw["sunset"],
                        "precip_sum":     raw["precipitation_sum"],
                        "precip_hours":   raw["precipitation_hours"],
                        "snowfall":       raw["snowfall_sum"],
                        "windspeed_max":  raw["wind_speed_10m_max"],
                        "windgusts_max":  raw["wind_gusts_10m_max"],
                        "raw":            json.dumps(raw),
                    },
                )
                inserted += cursor.rowcount

        return inserted


table = WeatherTable()
