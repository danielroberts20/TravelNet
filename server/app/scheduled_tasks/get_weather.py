"""
Retroactive weather fetch from Open-Meteo historical API.

Scheduled: daily at 05:30.
  - end_date   = today - WEATHER_LAG_DAYS   (configurable, default 3)
  - start_date = end_date - WEATHER_LOOKBACK_DAYS  (configurable, default 14)

Concurrency model: ThreadPoolExecutor with WEATHER_WORKERS threads fetches
cells in parallel. A shared RateLimiter enforces a target req/s ceiling that
scales with cell count (light loads stay gentle; heavy loads run faster).
Tenacity retries transient HTTP failures up to 3 times with exponential
back-off before giving up on an endpoint.

Skip-if-complete: cells already fully logged in weather_fetch_log are skipped
entirely. Partial completions (e.g. hourly OK but daily failed) retry only
the failed endpoints on the next run.

Can also be triggered manually:
  docker exec <container> python -m scheduled_tasks.get_weather
"""
from config.editable import load_overrides
load_overrides()

import time
import threading
import logging as stdlib_logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta

import requests
from tenacity import (
    Retrying,
    before_sleep_log,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from prefect import flow, task
from prefect.logging import get_run_logger

from config.general import (
    COORD_PRECISION, DAILY_VARS, HOURLY_VARS,
    OPEN_METEO_FORECAST_URL, OPEN_METEO_URL,
    WEATHER_FETCH_TIMEOUT, WEATHER_LAG_DAYS, WEATHER_LOOKBACK_DAYS,
    WEATHER_MAX_RPS, WEATHER_WORKERS,
)
from database.connection import get_conn, increment_api_usage
from database.weather.fetch_log import weather_fetch_log
from database.weather.table import table as weather_table
from notifications import notify_on_completion, record_flow_result

_module_logger = stdlib_logging.getLogger(__name__)


class RateLimiter:
    """Thread-safe token bucket. Enforces a maximum request rate across workers."""

    def __init__(self, rate: float) -> None:
        self._rate = rate
        self._interval = 1.0 / rate
        self._last = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            wait_for = self._interval - (now - self._last)
            if wait_for > 0:
                time.sleep(wait_for)
            self._last = time.monotonic()

    
def _get_rate(num_cells: int) -> float:
    """Return target req/s for the given cell count, capped at WEATHER_MAX_RPS."""
    if num_cells <= 50:
        rate = 1.0
    elif num_cells <= 200:
        rate = 2.0
    elif num_cells <= 500:
        rate = 3.0
    elif num_cells <= 1000:
        rate = 4.0
    else:
        rate = 5.0
    return min(rate, WEATHER_MAX_RPS)


def _wait_strategy(retry_state) -> float:
    """Short backoff for transient errors; long backoff for 429 rate limits."""
    exc = retry_state.outcome.exception()
    if (
        isinstance(exc, requests.HTTPError)
        and exc.response is not None
        and exc.response.status_code == 429
    ):
        return min(60.0 * (2 ** (retry_state.attempt_number - 1)), 300.0)  # 60s, 120s, 240s
    return min(2.0 * (2 ** (retry_state.attempt_number - 1)), 10.0)  # 2s, 4s, 8s


def _call_with_retry(fn, *args, **kwargs):
    """Call fn(*args, **kwargs) with up to 3 attempts.

    429 rate-limit responses back off for 60s, 120s, then 240s.
    Other RequestException errors back off for 2s, 4s, then 8s.
    Returns None if all attempts fail.
    """
    try:
        for attempt in Retrying(
            stop=stop_after_attempt(3),
            wait=_wait_strategy,
            retry=retry_if_exception_type(requests.RequestException),
            before_sleep=before_sleep_log(_module_logger, stdlib_logging.WARNING),
        ):
            with attempt:
                return fn(*args, **kwargs)
    except Exception:
        return None


def _fetch_hourly(lat: float, lon: float, start_date: date, end_date: date) -> dict:
    """Fetch hourly weather data from Open-Meteo for a single cell."""
    params = {
        "latitude":   lat,
        "longitude":  lon,
        "start_date": start_date.isoformat(),
        "end_date":   end_date.isoformat(),
        "hourly":     ",".join(HOURLY_VARS),
        "timezone":   "UTC",
    }
    resp = requests.get(OPEN_METEO_URL, params=params, timeout=WEATHER_FETCH_TIMEOUT)
    increment_api_usage(service="open-meteo")
    resp.raise_for_status()
    return resp.json()


def _fetch_uv(lat: float, lon: float, start_date: date, end_date: date) -> dict:
    """Fetch hourly uv_index from Open-Meteo historical forecast API."""
    params = {
        "latitude":   lat,
        "longitude":  lon,
        "start_date": start_date.isoformat(),
        "end_date":   end_date.isoformat(),
        "hourly":     "uv_index",
        "timezone":   "UTC",
    }
    resp = requests.get(OPEN_METEO_FORECAST_URL, params=params, timeout=WEATHER_FETCH_TIMEOUT)
    increment_api_usage(service="open-meteo")
    resp.raise_for_status()
    return resp.json()


def _fetch_daily(lat: float, lon: float, start_date: date, end_date: date) -> dict:
    """Fetch daily weather data from Open-Meteo for a single cell."""
    params = {
        "latitude":   lat,
        "longitude":  lon,
        "start_date": start_date.isoformat(),
        "end_date":   end_date.isoformat(),
        "daily":      DAILY_VARS,
        "timezone":   "UTC",
    }
    resp = requests.get(OPEN_METEO_URL, params=params, timeout=WEATHER_FETCH_TIMEOUT)
    increment_api_usage(service="open-meteo")
    resp.raise_for_status()
    return resp.json()


def _fetch_cell(
    lat: float,
    lon: float,
    cell_start: date,
    cell_end: date,
    limiter: RateLimiter,
) -> dict:
    """Fetch all three endpoints for one grid cell over its specific date range.
    IO only — no DB writes.
    """
    result = {
        "lat": lat, "lon": lon,
        "cell_start": cell_start, "cell_end": cell_end,
        "hourly_data": None, "uv_data": None, "daily_data": None,
        "error_hourly": None, "error_uv": None, "error_daily": None,
    }

    limiter.wait()
    data = _call_with_retry(_fetch_hourly, lat, lon, cell_start, cell_end)
    result["hourly_data"] = data
    if data is None:
        result["error_hourly"] = "All retry attempts failed"

    limiter.wait()
    data = _call_with_retry(_fetch_uv, lat, lon, cell_start, cell_end)
    result["uv_data"] = data
    if data is None:
        result["error_uv"] = "All retry attempts failed"
    elif result["hourly_data"] is not None:
        result["hourly_data"]["hourly"]["uv_index"] = (
            data.get("hourly", {}).get("uv_index", [])
        )

    limiter.wait()
    data = _call_with_retry(_fetch_daily, lat, lon, cell_start, cell_end)
    result["daily_data"] = data
    if data is None:
        result["error_daily"] = "All retry attempts failed"

    return result


def _contiguous_ranges(dates: list[date]) -> list[tuple[date, date]]:
    """Group a sorted list of dates into contiguous (start, end) ranges."""
    if not dates:
        return []
    ranges = []
    start = prev = dates[0]
    for d in dates[1:]:
        if (d - prev).days == 1:
            prev = d
        else:
            ranges.append((start, prev))
            start = prev = d
    ranges.append((start, prev))
    return ranges


@task
def fetch_weather_locations(
    start_date: date, end_date: date
) -> tuple[list[tuple[float, float, date, date]], int]:
    """Return (fetch_jobs, total_cells).

    fetch_jobs: list of (lat, lon, range_start, range_end) — the minimal
        date range to fetch per cell. Brand-new cells get the full window;
        partial cells get only their missing dates grouped into contiguous ranges.
    total_cells: total distinct cells in the window, for logging.
    """
    logger = get_run_logger()

    sql = """
        SELECT DISTINCT
            ROUND(latitude,  :p) AS latitude,
            ROUND(longitude, :p) AS longitude
        FROM location_unified
        WHERE DATE(timestamp) BETWEEN :start AND :end
          AND latitude  IS NOT NULL
          AND longitude IS NOT NULL
    """
    with get_conn(read_only=True) as conn:
        rows = conn.execute(sql, {
            "p":     COORD_PRECISION,
            "start": start_date.isoformat(),
            "end":   end_date.isoformat(),
        }).fetchall()

    all_cells   = {(r["latitude"], r["longitude"]) for r in rows}
    total_cells = len(all_cells)
    logger.info("Total distinct location cells in window: %d", total_cells)

    complete = weather_fetch_log.get_complete_cells(start_date, end_date)

    incomplete    = all_cells - complete
    missing_dates = weather_fetch_log.get_missing_dates_per_cell(start_date, end_date)

    all_dates = [
        start_date + timedelta(days=i)
        for i in range((end_date - start_date).days + 1)
    ]

    fetch_jobs: list[tuple[float, float, date, date]] = []
    for lat, lon in sorted(incomplete):
        cell_missing = missing_dates.get((lat, lon), all_dates)
        for range_start, range_end in _contiguous_ranges(sorted(cell_missing)):
            fetch_jobs.append((lat, lon, range_start, range_end))

    logger.info(
        "Cells to fetch: %d | Fetch jobs (date ranges): %d | "
        "Avg missing dates per cell: %.1f",
        len(incomplete),
        len(fetch_jobs),
        sum(
            (r - s).days + 1
            for _, _, s, r in fetch_jobs
        ) / len(fetch_jobs) if fetch_jobs else 0,
    )

    total_needed  = len(incomplete) * (end_date - start_date).days + 1
    total_fetched = sum((r - s).days + 1 for _, _, s, r in fetch_jobs)
    logger.info(
        "Cell-dates skipped (already in log): %d | Cell-dates to fetch: %d",
        total_needed - total_fetched,
        total_fetched,
    )

    return fetch_jobs, total_cells

@task
def fetch_and_store_all_weather(
    fetch_jobs: list[tuple[float, float, date, date]],
    total_cells: int,
    start_date: date,
    end_date: date,
) -> dict:
    logger = get_run_logger()

    rate = _get_rate(len(fetch_jobs))
    limiter = RateLimiter(rate=rate)
    logger.info(
        "Fetching %d jobs across %d cells with %d workers at %.1f req/s target.",
        len(fetch_jobs), total_cells, WEATHER_WORKERS, rate,
    )

    # --- Phase 1: parallel fetch (IO only, no DB) ---
    fetch_results = []
    with ThreadPoolExecutor(max_workers=WEATHER_WORKERS) as executor:
        futures = {
            executor.submit(_fetch_cell, lat, lon, cell_start, cell_end, limiter): (lat, lon, cell_start, cell_end)
            for lat, lon, cell_start, cell_end in fetch_jobs
        }
        for future in as_completed(futures):
            try:
                fetch_results.append(future.result())
            except Exception as exc:
                lat, lon, cell_start, cell_end = futures[future]
                logger.error(
                    "Unexpected error fetching (%.2f, %.2f) %s→%s: %s",
                    lat, lon, cell_start, cell_end, exc,
                )
                fetch_results.append({
                    "lat": lat, "lon": lon,
                    "cell_start": cell_start, "cell_end": cell_end,
                    "hourly_data": None, "uv_data": None, "daily_data": None,
                    "error_hourly": str(exc), "error_uv": str(exc), "error_daily": str(exc),
                })

    logger.info("Fetch phase complete (%d results). Writing to DB.", len(fetch_results))

    # --- Phase 2: sequential write (single thread) ---
    hourly_inserted = 0
    daily_inserted  = 0
    hourly_failed   = 0
    daily_failed    = 0
    uv_failed       = 0

    for r in fetch_results:
        lat, lon         = r["lat"], r["lon"]
        cell_start: date = r["cell_start"]
        cell_end: date   = r["cell_end"]
        hourly_ok        = r["hourly_data"] is not None
        uv_ok            = r["uv_data"] is not None
        daily_ok         = r["daily_data"] is not None
        h_rows           = None
        d_rows           = None

        if r["hourly_data"] is not None:
            h_rows = weather_table.insert_hourly_batch(r["hourly_data"], lat, lon)
            hourly_inserted += h_rows
        else:
            hourly_failed += 1

        if r["uv_data"] is None:
            uv_failed += 1

        if r["daily_data"] is not None:
            d_rows = weather_table.insert_daily_batch(r["daily_data"], lat, lon)
            daily_inserted += d_rows
        else:
            daily_failed += 1

        num_days = (cell_end - cell_start).days + 1
        for i in range(num_days):
            d = cell_start + timedelta(days=i)
            weather_fetch_log.record(
                lat=lat, lon=lon,
                date=d,
                hourly_ok=hourly_ok,
                uv_ok=uv_ok,
                daily_ok=daily_ok,
                hourly_rows=h_rows if i == 0 else None,
                daily_rows=d_rows if i == 0 else None,
                error_hourly=r["error_hourly"],
                error_uv=r["error_uv"],
                error_daily=r["error_daily"],
            )

        logger.debug(
            "(%.2f, %.2f) %s→%s: hourly_ok=%s uv_ok=%s daily_ok=%s",
            lat, lon, cell_start, cell_end, hourly_ok, uv_ok, daily_ok,
        )

    logger.info(
        "Weather fetch complete. Cells in window: %d | Jobs: %d | "
        "Hourly: %d inserted, %d failed | UV: %d failed | "
        "Daily: %d inserted, %d failed.",
        total_cells, len(fetch_jobs),
        hourly_inserted, hourly_failed,
        uv_failed,
        daily_inserted, daily_failed,
    )
    if hourly_failed or daily_failed or uv_failed:
        logger.warning(
            "%d hourly, %d UV, and %d daily job(s) failed after retries.",
            hourly_failed, uv_failed, daily_failed,
        )

    return {
        "num_locations": total_cells,
        "hourly_inserted": hourly_inserted,
        "daily_inserted": daily_inserted,
        "hourly_failed": hourly_failed,
        "daily_failed": daily_failed,
        "uv_failed": uv_failed,
    }


@flow(name="Get Weather", on_failure=[notify_on_completion])
def get_weather_flow():
    logger = get_run_logger()
    today      = date.today()
    end_date   = today - timedelta(days=WEATHER_LAG_DAYS)
    start_date = end_date - timedelta(days=WEATHER_LOOKBACK_DAYS)

    logger.info(
        "Weather fetch starting. Window: %s → %s",
        start_date.isoformat(), end_date.isoformat(),
    )

    fetch_jobs, total_cells = fetch_weather_locations(start_date, end_date)

    if not fetch_jobs:
        logger.info(
            "All %d cells complete for this window — nothing to fetch.",
            total_cells,
        )
        result = {
            "num_locations": total_cells, "hourly_inserted": 0, "daily_inserted": 0,
            "hourly_failed": 0, "daily_failed": 0, "uv_failed": 0,
        }
        record_flow_result(result)
        return result

    result = fetch_and_store_all_weather(fetch_jobs, total_cells, start_date, end_date)

    retention_days = WEATHER_LOOKBACK_DAYS + WEATHER_LAG_DAYS
    pruned = weather_fetch_log.prune_by_retention(retention_days)
    logger.info("Pruned %d stale weather_fetch_log entries.", pruned)

    record_flow_result(result)
    return result
