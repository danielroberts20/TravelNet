import os
from datetime import datetime

import requests
from dotenv import load_dotenv

load_dotenv()

ACCESS_KEY = os.environ["FX_API_KEY"]
UPLOAD_TOKEN = os.environ["UPLOAD_TOKEN"]
API_URL = "http://pi-server:8000"
FX_URL = "https://api.exchangerate.host/historical"

def upload_txt(text):
    target = os.path.join(API_URL, "upload_text")
    r = requests.post(target,
                      headers={
        "Content-Type": "text/plain",
        "Authorization": f"Bearer {UPLOAD_TOKEN}"
        },
                      data=text)
    return r.text

def upload_json(json):
    target = os.path.join(API_URL, "upload_json")
    r = requests.post(target,
                      headers={
                          "Content-Type": "application/json",
                          "Authorization": f"Bearer {UPLOAD_TOKEN}"
                      },
                      json=json)
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

#fx_data = get_fx_rate_at_date("2020-02-01", "GBP", "USD", "AUD", "NZD", source="EUR")