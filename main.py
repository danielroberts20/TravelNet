import calendar
import os
from datetime import datetime
import time

import requests
from dotenv import load_dotenv

load_dotenv()

ACCESS_KEY = os.environ["FX_API_KEY"]
UPLOAD_TOKEN = os.environ["UPLOAD_TOKEN"]
CURRENCIES = ["GBP", "USD", "CAN", "AUD", "NZD", "FJD", "THB", "KHR", "VND", "LAK"]
SOURCE = "GBP"
API_URL = "http://pi-server:8000"
FX_URL = "https://api.exchangerate.host/historical"

def upload_txt(text):
    target = os.path.join(API_URL, "upload_text")
    r = requests.post(target,
                      headers={"Content-Type": "text/plain", "Authorization": f"Bearer {UPLOAD_TOKEN}"},
                      data=text)
    return r.text

def upload_json(json):
    target = os.path.join(API_URL, "upload_json")
    r = requests.post(target,
                      headers={"Content-Type": "application/json", "Authorization": f"Bearer {UPLOAD_TOKEN}"},
                      data=json)
    return r.text

def upload_loc(json):
    target = os.path.join(API_URL, "upload_loc")
    r = requests.post(target,
                      headers={"Content-Type": "application/json", "Authorization": f"Bearer {UPLOAD_TOKEN}"},
                      data=json)
    return r.text

def upload_fx(json):
    target = os.path.join(API_URL, "upload_fx")
    r = requests.post(target,
                      headers={"Content-Type": "application/json", "Authorization": f"Bearer {UPLOAD_TOKEN}"},
                      data=json)
    return r.text

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
    return requests.get(f"{API_URL}/locations/recent?days={num_days}").json()

print(get_recent_locations(21))
