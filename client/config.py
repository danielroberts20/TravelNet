import os
from pathlib import Path

from yarl import URL # type: ignore

from dotenv import load_dotenv # type: ignore

load_dotenv()

ACCESS_KEY = os.environ["FX_API_KEY"]
UPLOAD_TOKEN = os.environ["UPLOAD_TOKEN"]
CURRENCIES = ["GBP", "USD", "CAN", "AUD", "NZD", "FJD", "THB", "KHR", "VND", "LAK"]
SOURCE = "GBP"
SERVER_URL = URL(os.environ["SERVER_URL"])
FX_URL = URL("https://api.exchangerate.host/historical")
DATA_DIR = Path("data")