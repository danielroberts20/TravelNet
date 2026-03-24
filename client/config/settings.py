import os
from dotenv import load_dotenv
from yarl import URL

load_dotenv()
UPLOAD_TOKEN = os.getenv("UPLOAD_TOKEN")
TRAVELNET_URL = URL(os.getenv("TRAVELNET_URL"))