import calendar
from datetime import datetime
import json
from dateutil.relativedelta import relativedelta
import logging
import time
from typing import Dict, Optional

import requests

from config.logging import configure_logging
from config.general import CURRENCIES, FX_API_KEY, FX_BACKUP_DIR, FX_URL, SOURCE_CURRENCY
from database.exchange.util import insert_fx_json

logger = logging.getLogger(__name__)

def get_fx_rate_at_date(date_string: str, retry_time: int = 5, *currencies: str, **kwargs: str) -> Optional[Dict]:
    """
    Get the FX rate at a date for all provided currencies.
    :param date_string: Date string in the format YYYY-MM-DD
    :param currencies: str ISO currency codes (e.g. "USD", "GBP")
    :param kwargs: if `source` is present it will be used as the source currency, otherwise `config.SOURCE_CURRENCY` is used by default
    :return: response JSON from the FX API or None if the date string is invalid
    """
    try:
        if currencies is None or len(list(currencies)) == 0:
            currencies = CURRENCIES
        datetime.strptime(date_string, "%Y-%m-%d")
        params = {
            "access_key": FX_API_KEY,
            "date": date_string,
            "source": kwargs.get("source", SOURCE_CURRENCY),
            "currencies": ",".join(list(currencies)),
        }
        response = requests.get(FX_URL, params=params)
        if response.json().get("success") is not True or "error" in response.json():
            if response.json().get("error", {}).get("type", {}) == "rate_limit_reached":
                logger.warning(f"FX API rate limit reached. Re-trying in {retry_time} seconds...")
                time.sleep(retry_time)
                return get_fx_rate_at_date(date_string, retry_time, *currencies, **kwargs) # Recursive
            else:
                logger.error(f"FX API error: {response.json().get('error')}")
                return None
        else:
            logger.info(f"Successful FX API call for {date_string}")
            return response.json()
    except ValueError:
        return None
    
def get_fx_for_month(month=None, year=None):
    """
    Get daily FX rates for currencies specified by config.
    :param month: int (1-12 inc.). If no month is specified, previous month is used.
    :param year: int (1970<=). If no year is specified, current year is used.
    :return:
    """
    now = datetime.now()
    month = now.month - 1 if month is None else month
    month = max(1, min(month, 12)) # Clamp between 1-12

    responses = []

    year = now.year if year is None else year
    year = max(1970, min(year, now.year)) # Clamp between 1970-current year
    for day in range(1, calendar.monthrange(year, month)[1]+1): # Loop through days in month
        logger.info(f"Getting FX for {year}-{month:02}-{day:02}...")
        fx_data = get_fx_rate_at_date(f"{year}-{month:02}-{day:02}") # Get FX for day (defaults to config currencies and source currency)
        if fx_data is None or fx_data.get("success") is not True:
            logger.error(f"Failed to fetch FX for {year}-{month:02}-{day:02}")
            continue

        responses.append(fx_data)
        insert_fx_json(fx_data)

    logger.info(f"Done fetching FX rates for {year}-{month:02}")

    with open(FX_BACKUP_DIR / f"{year}-{month:02}.json", "w") as f:
        json.dump(responses, f, indent=2)
    logger.info(f"Successfully saved FX rates to {FX_BACKUP_DIR / f'{year}-{month:02}.json'}")

def run():
    configure_logging()
    logger.info(f"Getting FX rates for previous month ({(datetime.now() - relativedelta(months=1)).strftime('%B')})...")
    get_fx_for_month()

run()