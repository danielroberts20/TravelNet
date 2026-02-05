import json
import os
from datetime import datetime
from io import BytesIO

import requests
from dotenv import load_dotenv

load_dotenv()

ACCESS_KEY = os.environ["FX_API_KEY"]
UPLOAD_TOKEN = os.environ["UPLOAD_TOKEN"]
API_URL = "http://100.125.97.105:8000/upload"
FX_URL = "https://api.exchangerate.host/historical"

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

def upload_json(json_data):
    fx_bytes = BytesIO(json.dumps(json_data).encode("utf-8"))
    r = requests.post(
        API_URL,
        headers={
            "Authorization": f"Bearer {UPLOAD_TOKEN}"},
        files={"file": ("fx.json", fx_bytes, "application/json")}
    )
    return r.status_code

fx_data = get_fx_rate_at_date("2018-01-01", "GBP", "USD", "AUD", "NZD", source="EUR")

print(upload_json(fx_data))