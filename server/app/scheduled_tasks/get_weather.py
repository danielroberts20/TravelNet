"""
Retroactive weather fetch from Open-Meteo historical API.

Scheduled: 14th of each month at 04:00.
  - end_date   = today - 7 days  (conservative lag buffer)
  - start_date = end_date - 40 days  (covers full previous month + overlap)

Idempotent: uses INSERT OR IGNORE against UNIQUE(timestamp, latitude, longitude).
Can also be triggered manually:
  docker exec <container> python -m scheduled_tasks.get_weather

API endpoint: https://archive-api.open-meteo.com/v1/archive
Variables fetched (hourly):
  temperature_2m, precipitation, windspeed_10m, weathercode
"""

import json
import logging
import time
from datetime import date, datetime, timedelta, timezone

import requests

from config.general import COORD_PRECISION, HOURLY_VARS, OPEN_METEO_URL, REQUEST_DELAY
from config.logging import configure_logging
from config.settings import settings
from database.util import get_conn
from notifications import CronJobMailer

logger = logging.getLogger(__name__)



def _get_distinct_locations(start_date: date, end_date: date) -> list[tuple[float, float]]:
    """
    Return distinct (rounded_lat, rounded_lon) pairs from location_unified
    that fall within the fetch window.
    """
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


def _fetch_weather(lat: float, lon: float, start_date: date, end_date: date) -> dict | None:
    """Call Open-Meteo and return the parsed JSON, or None on failure."""
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
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as exc:
        logger.warning("Open-Meteo request failed for (%.2f, %.2f): %s", lat, lon, exc)
        return None


def _insert_weather_rows(data: dict, lat: float, lon: float) -> int:
    """
    Parse Open-Meteo hourly response and insert rows into the weather table.
    Opens and closes its own connection per cell to avoid long-held locks.
    Returns the number of rows newly inserted (skips duplicates silently).
    """
    hourly = data.get("hourly", {})
    times          = hourly.get("time", [])
    temperatures   = hourly.get("temperature_2m", [])
    precipitations = hourly.get("precipitation", [])
    windspeeds     = hourly.get("windspeed_10m", [])
    weathercodes   = hourly.get("weathercode", [])

    fetched_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    inserted = 0

    with get_conn() as conn:
        for i, ts in enumerate(times):
            raw = {
                "temperature_2m": temperatures[i]   if i < len(temperatures)   else None,
                "precipitation":  precipitations[i]  if i < len(precipitations) else None,
                "windspeed_10m":  windspeeds[i]      if i < len(windspeeds)     else None,
                "weathercode":    weathercodes[i]    if i < len(weathercodes)   else None,
            }
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO weather
                    (fetched_at, timestamp, latitude, longitude,
                     temperature_c, precipitation_mm, windspeed_kmh, weathercode, raw_json)
                VALUES
                    (:fetched_at, :timestamp, :lat, :lon,
                     :temp, :precip, :wind, :code, :raw)
                """,
                {
                    "fetched_at": fetched_at,
                    "timestamp":  ts,
                    "lat":        lat,
                    "lon":        lon,
                    "temp":       raw["temperature_2m"],
                    "precip":     raw["precipitation"],
                    "wind":       raw["windspeed_10m"],
                    "code":       raw["weathercode"],
                    "raw":        json.dumps(raw),
                },
            )
            inserted += cursor.rowcount

    return inserted


def run():
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

    total_inserted = 0
    failed = 0

    for lat, lon in locations:
        data = _fetch_weather(lat, lon, start_date, end_date)
        if data is None:
            failed += 1
            continue

        n = _insert_weather_rows(data, lat, lon)
        total_inserted += n
        logger.debug("(%.2f, %.2f): %d rows inserted.", lat, lon, n)
        time.sleep(REQUEST_DELAY)

    logger.info(
        "Weather fetch complete. Cells: %d, new rows: %d, failed cells: %d.",
        len(locations), total_inserted, failed,
    )
    if failed:
        logger.warning("%d location cell(s) failed to fetch from Open-Meteo.", failed)
        
    return {
        "num_locations": len(locations),
        "inserted": total_inserted
    }

if __name__ == "__main__":
    configure_logging()

    with CronJobMailer("get_weather", settings.smtp_config) as job:
        result = run()
        job.add_metric("num locations", result["num_locations"])
        job.add_metric("inserted", result["inserted"])