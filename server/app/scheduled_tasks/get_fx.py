from config.editable import load_overrides
load_overrides()

import calendar
from datetime import datetime
import json
from dateutil.relativedelta import relativedelta
import logging
import time
from config.editable import load_overrides
from typing import Dict, Optional
import requests
from database.util import increment_api_usage
from config.logging import configure_logging
from config.general import CURRENCIES, FX_BACKUP_DIR, FX_URL, SOURCE_CURRENCY
from config.settings import settings
from database.exchange.util import insert_fx_json
from notifications import CronJobMailer

logger = logging.getLogger(__name__)


def _extract_quotes(response: dict) -> dict:
    """Extract quotes from API response, checking both 'quotes' and 'rates' keys."""
    return response.get("quotes") or response.get("rates") or {}


def get_fx_rate_at_date(date_string: str, retry_time: int = 5, *currencies: str, **kwargs: str) -> Optional[Dict]:
    """
    Get the FX rate at a date for all provided currencies.
    :param date_string: Date string in the format YYYY-MM-DD
    :param currencies: str ISO currency codes (e.g. "USD", "GBP")
    :param kwargs: if `source` is present it will be used as the source currency,
                   otherwise `config.SOURCE_CURRENCY` is used by default
    :return: response JSON from the FX API or None if the date string is invalid
    """
    try:
        if currencies is None or len(list(currencies)) == 0:
            currencies = CURRENCIES
        datetime.strptime(date_string, "%Y-%m-%d")
        params = {
            "access_key": settings.fx_api_key,
            "date": date_string,
            "source": kwargs.get("source", SOURCE_CURRENCY),
            "currencies": ",".join(list(currencies)),
        }
        response = requests.get(FX_URL, params=params)
        increment_api_usage("exchangerate.host")
        if response.json().get("success") is not True or "error" in response.json():
            if response.json().get("error", {}).get("type", {}) == "rate_limit_reached":
                logger.warning(f"FX API rate limit reached. Re-trying in {retry_time} seconds...")
                time.sleep(retry_time)
                return get_fx_rate_at_date(date_string, retry_time, *currencies, **kwargs)
            else:
                logger.error(f"FX API error: {response.json().get('error')}")
                return None
        else:
            logger.info(f"Successful FX API call for {date_string}")
            return response.json()
    except ValueError:
        return None


def get_fx_for_month(month: int = None, year: int = None) -> dict:
    """
    Fetch and store daily FX rates for an entire month in a single API call.
    :param month: 1-12, defaults to previous month
    :param year: defaults to current year
    :return: dict with keys: month_label, dates_inserted, backup_path
    :raises RuntimeError: if the API returns an error or no quotes
    """
    now = datetime.now()
    month = max(1, min(month if month is not None else now.month - 1, 12))
    year = max(1970, min(year if year is not None else now.year, now.year))
    start_date = f"{year}-{month:02}-01"
    end_date = f"{year}-{month:02}-{calendar.monthrange(year, month)[1]:02}"
    month_label = f"{year}-{month:02}"

    logger.info(f"Fetching FX rates for {month_label} ({start_date} to {end_date})...")

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
        error = response.get("error")
        logger.error(f"FX API error for {month_label}: {error}")
        raise RuntimeError(f"FX API returned an error for {month_label}: {error}")

    quotes = _extract_quotes(response)
    if not quotes:
        logger.warning(f"No quotes returned for {month_label}")
        raise RuntimeError(f"FX API returned no quotes for {month_label}")

    insert_fx_json(quotes)

    backup_path = FX_BACKUP_DIR / f"{datetime.now().strftime('%Y-%m')}.json"
    with open(backup_path, "w") as f:
        json.dump(response, f, indent=2)
    logger.info(f"Saved FX rates to {backup_path}")

    return {
        "month_label": month_label,
        "dates_inserted": len(quotes),
        "backup_path": str(backup_path),
    }


if __name__ == "__main__":
    configure_logging()

    prev_month = datetime.now() - relativedelta(months=1)
    logger.info(f"Getting FX rates for previous month ({prev_month.strftime('%B %Y')})...")

    with CronJobMailer("get_fx", settings.smtp_config,
                       detail="Pull FX rates for previous month") as job:
        result = get_fx_for_month()
        job.add_metric("month", result["month_label"])
        job.add_metric("dates inserted", result["dates_inserted"])
        job.add_metric("backup path", result["backup_path"])