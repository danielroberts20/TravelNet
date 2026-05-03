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

import time
from datetime import date, timedelta

import requests

from prefect import task, flow
from prefect.logging import get_run_logger

from config.general import COORD_PRECISION, DAILY_VARS, HOURLY_VARS, OPEN_METEO_URL, REQUEST_DELAY
from database.connection import get_conn, increment_api_usage
from database.weather.table import table as weather_table
from notifications import notify_on_completion, record_flow_result


def _fetch_hourly(lat: float, lon: float, start_date: date, end_date: date, logger) -> dict | None:
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


def _fetch_daily(lat: float, lon: float, start_date: date, end_date: date, logger) -> dict | None:
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


@task
def fetch_weather_locations(start_date: date, end_date: date) -> list[tuple[float, float]]:
    logger = get_run_logger()
    sql = """
        SELECT DISTINCT
            ROUND(latitude, :p) AS latitude,
            ROUND(longitude, :p) AS longitude
        FROM location_unified
        WHERE DATE(timestamp) BETWEEN :start AND :end
          AND latitude IS NOT NULL
          AND longitude IS NOT NULL
    """
    with get_conn(read_only=True) as conn:
        rows = conn.execute(sql, {
            "p": COORD_PRECISION,
            "start": start_date.isoformat(),
            "end": end_date.isoformat(),
        }).fetchall()
    locations = [(row["latitude"], row["longitude"]) for row in rows]
    logger.info("Distinct location cells to fetch: %d", len(locations))
    return locations


@task
def fetch_and_store_all_weather(
    locations: list[tuple[float, float]],
    start_date: date,
    end_date: date,
) -> dict:
    logger = get_run_logger()
    hourly_inserted = 0
    daily_inserted  = 0
    hourly_failed   = 0
    daily_failed    = 0

    for lat, lon in locations:
        # Hourly
        hourly_data = _fetch_hourly(lat, lon, start_date, end_date, logger)
        if hourly_data is None:
            hourly_failed += 1
        else:
            hourly_inserted += weather_table.insert_hourly_batch(hourly_data, lat, lon)
        time.sleep(REQUEST_DELAY)

        # Daily
        daily_data = _fetch_daily(lat, lon, start_date, end_date, logger)
        if daily_data is None:
            daily_failed += 1
        else:
            daily_inserted += weather_table.insert_daily_batch(daily_data, lat, lon)
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
        "daily_failed": daily_failed,
    }


@flow(name="Get Weather", on_failure=[notify_on_completion])
def get_weather_flow():
    logger = get_run_logger()
    today      = date.today()
    end_date   = today - timedelta(days=7)
    start_date = end_date - timedelta(days=40)

    logger.info(
        "Weather fetch starting. Window: %s → %s",
        start_date.isoformat(), end_date.isoformat(),
    )

    locations = fetch_weather_locations(start_date, end_date)

    if not locations:
        logger.info("No location data in window — nothing to fetch.")
        result = {"num_locations": 0, "hourly_inserted": 0, "daily_inserted": 0,
                  "hourly_failed": 0, "daily_failed": 0}
        record_flow_result(result)
        return result

    result = fetch_and_store_all_weather(locations, start_date, end_date)
    record_flow_result(result)
    return result
