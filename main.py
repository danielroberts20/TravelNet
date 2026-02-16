import calendar
import time
from datetime import datetime

import requests
from config import ACCESS_KEY, FX_URL, CURRENCIES, SOURCE, SERVER_URL
from server import upload_fx

def get_fx_rate_at_date(date_string, *currencies, **kwargs):
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
    now = datetime.now()
    month = now.month - 1 if month is None else month
    year = now.year if year is None else year
    for day in range(1, calendar.monthrange(year, month)[1]+1):
        upload_fx(get_fx_rate_at_date(f"{year}-{month:02}-{day:02}", *CURRENCIES, source=SOURCE))
        time.sleep(2.5)

def get_recent_locations(num_days=7):
    return requests.get(f"{SERVER_URL}/locations/recent?days={num_days}").json()

print(get_recent_locations(21))
