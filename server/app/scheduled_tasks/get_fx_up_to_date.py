# scheduled_tasks/get_fx_up_to_date.py
from config.editable import load_overrides
load_overrides()

import json
from datetime import date, datetime, timedelta

import requests

from prefect import task, flow
from prefect.logging import get_run_logger

from config.general import CURRENCIES, FX_BACKUP_DIR, FX_TIMEFRAME_URL, SOURCE_CURRENCY
from config.settings import settings
from database.exchange.fx import get_api_usage, insert_fx_json
from database.connection import get_conn, increment_api_usage
from notifications import notify_on_completion



def _get_missing_dates(target_date: date, logger) -> list[str]:
    """
    Return sorted list of dates (YYYY-MM-DD) missing from fx_rates
    between the earliest date in the DB and target_date.
    """
    with get_conn(read_only=True) as conn:
        rows = conn.execute(
            "SELECT DISTINCT date FROM fx_rates WHERE source_currency = ?",
            (SOURCE_CURRENCY,)
        ).fetchall()

    if not rows:
        logger.warning("No existing FX data in DB — run get_fx_for_month first")
        return []

    dates_in_db = {row["date"] for row in rows}
    earliest = date.fromisoformat(min(dates_in_db))

    expected = {
        (earliest + timedelta(days=i)).isoformat()
        for i in range((target_date - earliest).days + 1)
    }

    return sorted(expected - dates_in_db)


@task
def check_fx_api_quota() -> int:
    """Return remaining API calls this month. Raises RuntimeError if unknown."""
    logger = get_run_logger()
    used = get_api_usage("exchangerate.host").get("count")
    if used is None:
        raise RuntimeError("Could not verify API quota, aborting")
    remaining = 100 - used
    if remaining < 1:
        raise RuntimeError("No API quota remaining, aborting")
    logger.info("%d API call(s) remaining this month", remaining)
    return remaining


@task
def get_missing_fx_dates(target_date: date) -> list[str]:
    logger = get_run_logger()
    missing = _get_missing_dates(target_date, logger)
    if not missing:
        logger.info("No missing dates found up to %s, nothing to do", target_date)
    else:
        logger.info(
            "%d missing date(s) between %s and %s, fetching...",
            len(missing), missing[0], missing[-1],
        )
    return missing


@task(retries=3, retry_delay_seconds=10)
def fetch_fx_timeframe(start_date: str, end_date: str) -> dict:
    logger = get_run_logger()
    params = {
        "access_key": settings.fx_api_key,
        "start_date": start_date,
        "end_date": end_date,
        "source": SOURCE_CURRENCY,
        "currencies": ",".join(CURRENCIES),
    }

    resp = requests.get(str(FX_TIMEFRAME_URL), params=params, timeout=30)
    resp.raise_for_status()
    response = resp.json()
    increment_api_usage("exchangerate.host")

    if response.get("success") is not True:
        raise RuntimeError(f"API error: {response.get('error')}")

    quotes = response.get("quotes") or response.get("rates") or {}
    if not quotes:
        raise RuntimeError(f"No quotes returned for {start_date} to {end_date}")

    logger.info("Fetched %d date(s) from API", len(quotes))
    return response


@task
def store_fx_and_backup(response: dict) -> dict:
    logger = get_run_logger()
    quotes = response.get("quotes") or response.get("rates") or {}

    insert_fx_json(quotes)
    logger.info("Successfully backfilled %d date(s)", len(quotes))

    backup_path = FX_BACKUP_DIR / f"backfill_{datetime.now().strftime('%Y-%m-%d')}.json"
    with open(backup_path, "w") as f:
        json.dump(response, f, indent=2)
    logger.info("Saved backup to %s", backup_path)

    return {
        "dates_inserted": len(quotes),
        "backup_path": str(backup_path),
    }


@flow(name="Backfill FX", on_completion=[notify_on_completion], on_failure=[notify_on_completion])
def get_fx_up_to_date_flow(target_date: date | None = None):
    logger = get_run_logger()
    target_date = target_date or date.today()

    check_fx_api_quota()

    missing_dates = get_missing_fx_dates(target_date)
    if not missing_dates:
        return {"start_date": "", "end_date": "", "dates_inserted": 0, "backup_path": ""}

    start_date = missing_dates[0]
    end_date = missing_dates[-1]

    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    if (end - start).days > 365:
        raise RuntimeError(
            f"Date range exceeds 365 day API limit "
            f"({(end - start).days} days) — use get_fx_for_month to backfill manually"
        )

    response = fetch_fx_timeframe(start_date, end_date)
    result = store_fx_and_backup(response)

    return {
        "start_date": start_date,
        "end_date": end_date,
        **result,
    }
