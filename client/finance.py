import calendar
import time
from datetime import datetime

import requests

from config import ACCESS_KEY, FX_URL, CURRENCIES, SOURCE
from server_interaction import upload_fx


def get_fx_rate_at_date(date_string, *currencies, **kwargs):
    """
    Get the FX rate at a date for all provided currencies.
    :param date_string: Date string in the format YYYY-MM-DD
    :param currencies: str ISO currency codes (e.g. "USD", "GBP")
    :param kwargs: if `source` is present it will be used as the source currency, otherwise GBP is used by default
    :return:
    """
    try:
        datetime.strptime(date_string, "%Y-%m-%d")
        params = {
            "access_key": ACCESS_KEY,
            "date": date_string,
            "source": kwargs.get("source") or "GBP",
            "currencies": ",".join(list(currencies)),
        }
        response = requests.get(FX_URL, params=params)
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
    for day in range(1, calendar.monthrange(year, month)[1]+1):
        upload_fx(get_fx_rate_at_date(f"{year}-{month:02}-{day:02}", *CURRENCIES, source=SOURCE))
        time.sleep(2.5)