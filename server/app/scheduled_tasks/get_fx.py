import calendar
from datetime import datetime
from dateutil.relativedelta import relativedelta
import logging
import time
from typing import Dict, Optional

import requests

from config.logging import configure_logging
from database.exchange import insert_fx_rate
from config.general import CURRENCIES, FX_API_KEY, FX_URL, SOURCE_CURRENCY

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

    year = now.year if year is None else year
    year = max(1970, min(year, now.year)) # Clamp between 1970-current year
    for day in range(1, calendar.monthrange(year, month)[1]+1): # Loop through days in month
        logger.info(f"Getting FX for {year}-{month:02}-{day:02}...")
        fx_data = get_fx_rate_at_date(f"{year}-{month:02}-{day:02}") # Get FX for day (defaults to config currencies and source currency)
        if fx_data is None or fx_data.get("success") is not True:
            logger.error(f"Failed to fetch FX for {year}-{month:02}-{day:02}")
            continue
        quotes = fx_data.get("quotes", {})
        for source_target_pair in quotes.keys(): # Loop through returned FX rates (e.g. "GBPUSD", "GBPCAD" etc.)
            source_currency, target_currency = source_target_pair[:3], source_target_pair[3:] # Extract source and target currency from pair string
            if source_currency != SOURCE_CURRENCY: # Sanity check - should always be the same as config.SOURCE_CURRENCY
                logger.warning(f"Unexpected source currency in FX data: {source_currency} (expected {SOURCE_CURRENCY})")
                continue
            rate = quotes[source_target_pair] # Extract rate from response
            insert_fx_rate(
                date=f"{year}-{month:02}-{day:02}", 
                source_currency=source_currency, 
                target_currency=target_currency, 
                rate=float(rate),
                ts=int(fx_data.get("timestamp"))) # Insert into DB
    logger.info(f"Done fetching FX rates for {year}-{month:02}")

def run():
    configure_logging()
    logger.info(f"Getting FX rates for previous month ({(datetime.now() - relativedelta(months=1)).strftime('%B')})...")
    get_fx_for_month()

run()