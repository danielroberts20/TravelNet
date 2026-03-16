# scheduled_tasks/get_fx_up_to_date.py

import json
import logging
from datetime import date, timedelta

import requests

from config.general import CURRENCIES, FX_BACKUP_DIR, FX_URL, SOURCE_CURRENCY
from config.settings import settings
from config.logging import configure_logging
from database.exchange.util import get_api_usage, increment_api_usage, insert_fx_json
from database.util import get_conn
from notifications import CronJobMailer

logger = logging.getLogger(__name__)

def _get_missing_dates(target_date: date) -> list[str]:
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


def get_fx_up_to_date(target_date: date = None):
    """
    Backfill any missing FX rates from the earliest date in the DB up to target_date.
    Uses a single timeframe API call spanning the earliest to latest missing date.
    :param target_date: date to backfill up to (defaults to today)
    """
    target_date = target_date or date.today()

    # Check quota before doing anything
    used = get_api_usage("exchangerate.host").get("count")
    remaining = None if used is None else 100 - used
    if remaining is None:
        logger.error("Could not verify API quota, aborting")
        return None
    if remaining < 1:
        logger.error(f"No API quota remaining, aborting")
        return None
    logger.info(f"{remaining} API call(s) remaining this month")

    missing_dates = _get_missing_dates(target_date)
    if not missing_dates:
        logger.info(f"No missing dates found up to {target_date}, nothing to do")
        return None

    start_date = missing_dates[0]
    end_date = missing_dates[-1]

    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    if (end - start).days > 365:
        logger.error(
            f"Date range exceeds 365 day API limit "
            f"({(end - start).days} days) — use get_fx_for_month to backfill manually"
        )
        return None

    logger.info(
        f"{len(missing_dates)} missing date(s) between "
        f"{start_date} and {end_date}, fetching..."
    )

    params = {
        "access_key": settings.fx_api_key,
        "start_date": start_date,
        "end_date": end_date,
        "source": SOURCE_CURRENCY,
        "currencies": ",".join(CURRENCIES),
    }

    response = requests.get(FX_URL, params=params).json()
    increment_api_usage("exchangerate.host")

    if response.get("success") is not True:
        logger.error(f"API error: {response.get('error')}")
        return None

    quotes = response.get("quotes") or response.get("rates") or {}
    if not quotes:
        logger.warning(f"No quotes returned for {start_date} to {end_date}")
        return None

    insert_fx_json(quotes)
    logger.info(f"Successfully backfilled {len(quotes)} date(s)")

    backup_path = FX_BACKUP_DIR / f"backfill_{date.today().isoformat()}.json"
    with open(backup_path, "w") as f:
        json.dump(response, f, indent=2)
    logger.info(f"Saved backup to {backup_path}")

    return {
        "start_date": start_date,
        "end_date": end_date,
        "dates_inserted": len(quotes),
        "backup_path": str(backup_path),
    }


if __name__ == "__main__":
    configure_logging()

    logger.info("Running get_fx_up_to_date...")

    with CronJobMailer("get_fx_up_to_date", settings.smtp_config()) as job:
        result = get_fx_up_to_date()
        if result is None:
            raise RuntimeError("get_fx_up_to_date failed — see logs for details")
        
        job.add_metric("start date", result["start_date"])
        job.add_metric("end date", result["end_date"])
        job.add_metric("dates inserted", result["dates_inserted"])
        job.add_metric("backup path", result["backup_path"])