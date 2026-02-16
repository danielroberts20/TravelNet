import os

from dotenv import load_dotenv

load_dotenv()

ACCESS_KEY = os.environ["FX_API_KEY"]
UPLOAD_TOKEN = os.environ["UPLOAD_TOKEN"]
CURRENCIES = ["GBP", "USD", "CAN", "AUD", "NZD", "FJD", "THB", "KHR", "VND", "LAK"]
SOURCE = "GBP"
SERVER_URL = "http://pi-server:8000"
FX_URL = "https://api.exchangerate.host/historical"