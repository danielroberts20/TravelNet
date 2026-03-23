"""
Retroactive weather fetch from Open-Meteo historical API.

Scheduled: 14th of each month at 04:00.
  - end_date   = today - 7 days  (conservative lag buffer)
  - start_date = end_date - 40 days  (covers full previous month + overlap)

Idempotent: uses INSERT OR IGNORE against UNIQUE constraints on both tables.
Can also be triggered manually:
  docker exec <container> python -m scheduled_tasks.get_weather

Hourly endpoint: https://archive-api.open-meteo.com/v1/archive
Daily endpoint:  https://archive-api.open-meteo.com/v1/archive (same, different params)
"""
from config.editable import load_overrides
load_overrides()

import json
import logging
import time
from datetime import date, datetime, timedelta, timezone
import requests
from config.general import COORD_PRECISION, DAILY_VARS, HOURLY_VARS, OPEN_METEO_URL, REQUEST_DELAY
from config.logging import configure_logging
from config.settings import settings
from database.util import get_conn, increment_api_usage
from notifications import CronJobMailer

logger = logging.getLogger(__name__)



def _get_distinct_locations(start_date: date, end_date: date) -> list[tuple[float, float]]:
    """Return distinct (rounded_lat, rounded_lon) pairs from location_unified in the window."""
    sql = """
        SELECT DISTINCT
            ROUND(lat, :p) AS lat,
            ROUND(lon, :p) AS lon
        FROM location_unified
        WHERE DATE(timestamp) BETWEEN :start AND :end
          AND lat IS NOT NULL
          AND lon IS NOT NULL
    """
    with get_conn(read_only=True) as conn:
        rows = conn.execute(sql, {
            "p": COORD_PRECISION,
            "start": start_date.isoformat(),
            "end": end_date.isoformat(),
        }).fetchall()
    return [(row["lat"], row["lon"]) for row in rows]


def _fetch_hourly(lat: float, lon: float, start_date: date, end_date: date) -> dict | None:
    """Fetch hourly weather data from Open-Meteo for a single cell."""
    params = {
        "latitude":   lat,
        "longitude":  lon,
        "start_date": start_date.isoformat(),
        "end_date":   end_date.isoformat(),
        "hourly":     ",".join(HOURLY_VARS),
        "timezone":   "UTC",
    }
    try:
        resp = requests.get(OPEN_METEO_URL, params=params, timeout=30)
        increment_api_usage(service="open-meteo")
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as exc:
        logger.warning("Hourly fetch failed for (%.2f, %.2f): %s", lat, lon, exc)
        return None


def _fetch_daily(lat: float, lon: float, start_date: date, end_date: date) -> dict | None:
    """Fetch daily weather data from Open-Meteo for a single cell."""
    params = {
        "latitude":   lat,
        "longitude":  lon,
        "start_date": start_date.isoformat(),
        "end_date":   end_date.isoformat(),
        "daily":      DAILY_VARS,
        "timezone":   "UTC",
    }
    try:
        resp = requests.get(OPEN_METEO_URL, params=params, timeout=30)
        increment_api_usage(service="open-meteo")
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as exc:
        logger.warning("Daily fetch failed for (%.2f, %.2f): %s", lat, lon, exc)
        return None


def _insert_hourly_rows(data: dict, lat: float, lon: float) -> int:
    """Insert hourly rows into weather_hourly. Returns count of newly inserted rows."""
    hourly = data.get("hourly", {})
    times                = hourly.get("time", [])
    temperatures         = hourly.get("temperature_2m", [])
    apparent_temperatures = hourly.get("apparent_temperature", [])
    precipitations       = hourly.get("precipitation", [])
    windspeeds           = hourly.get("windspeed_10m", [])
    winddirections       = hourly.get("winddirection_10m", [])
    weathercodes         = hourly.get("weathercode", [])
    uv_indices           = hourly.get("uv_index", [])
    cloudcovers          = hourly.get("cloudcover", [])
    is_days              = hourly.get("is_day", [])

    fetched_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
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
                    "timestamp":     ts,
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


def _insert_daily_rows(data: dict, lat: float, lon: float) -> int:
    """Insert daily rows into weather_daily. Returns count of newly inserted rows."""
    daily = data.get("daily", {})
    dates              = daily.get("time", [])
    sunrises           = daily.get("sunrise", [])
    sunsets            = daily.get("sunset", [])
    precip_sums        = daily.get("precipitation_sum", [])
    precip_hours       = daily.get("precipitation_hours", [])
    snowfall_sums      = daily.get("snowfall_sum", [])
    windspeed_maxes    = daily.get("wind_speed_10m_max", [])
    windgust_maxes     = daily.get("wind_gusts_10m_max", [])

    fetched_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
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


def run() -> dict:
    """Fetch and store retroactive weather data for all distinct location cells.

    Covers the window (today - 47 days) → (today - 7 days).  The 7-day lag
    gives Open-Meteo time to finalise archive data.  Returns a summary dict
    with counts of cells processed and rows inserted/failed.
    """
    today      = date.today()
    end_date   = today - timedelta(days=7)
    start_date = end_date - timedelta(days=40)

    logger.info(
        "Weather fetch starting. Window: %s → %s",
        start_date.isoformat(), end_date.isoformat(),
    )

    locations = _get_distinct_locations(start_date, end_date)
    logger.info("Distinct location cells to fetch: %d", len(locations))

    if not locations:
        logger.info("No location data in window — nothing to fetch.")
        return

    hourly_inserted = 0
    daily_inserted  = 0
    hourly_failed   = 0
    daily_failed    = 0

    for lat, lon in locations:
        # Hourly
        hourly_data = _fetch_hourly(lat, lon, start_date, end_date)
        if hourly_data is None:
            hourly_failed += 1
        else:
            hourly_inserted += _insert_hourly_rows(hourly_data, lat, lon)
        time.sleep(REQUEST_DELAY)

        # Daily
        daily_data = _fetch_daily(lat, lon, start_date, end_date)
        if daily_data is None:
            daily_failed += 1
        else:
            daily_inserted += _insert_daily_rows(daily_data, lat, lon)
        time.sleep(REQUEST_DELAY)

        logger.debug("(%.2f, %.2f): done.", lat, lon)

    logger.info(
        "Weather fetch complete. Cells: %d | "
        "Hourly: %d inserted, %d failed | "
        "Daily: %d inserted, %d failed.",
        len(locations),
        hourly_inserted, hourly_failed,
        daily_inserted, daily_failed,
    )
    if hourly_failed or daily_failed:
        logger.warning(
            "%d hourly and %d daily cell(s) failed to fetch from Open-Meteo.",
            hourly_failed, daily_failed,
        )

    return {
        "num_locations": len(locations),
        "hourly_inserted": hourly_inserted,
        "daily_inserted": daily_inserted,
        "hourly_failed": hourly_failed,
        "daily_failed": daily_failed
    }

if __name__ == "__main__":
    configure_logging()

    with CronJobMailer("get_weather", settings.smtp_config,
                       detail="Get weather day for previous 40 days, starting from the 7th") as job:
        result = run()
        job.add_metric("num locations", result["num_locations"])
        job.add_metric("hourly inserted", result["hourly_inserted"])
        job.add_metric("daily inserted", result["daily_inserted"])
        job.add_metric("hourly failed", result["hourly_failed"])
        job.add_metric("daily failed", result["daily_failed"])