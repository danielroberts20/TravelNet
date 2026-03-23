"""
config/general.py
~~~~~~~~~~~~~~~~~
Static paths, constants, and editable runtime configuration for TravelNet.

All values that should be tunable at runtime are registered with the
@editable() decorator so they appear in the /metadata/config endpoint and can
be overridden via config_overrides.json without a code change.

Directory constants are created on module import (mkdir exist_ok=True) so
the application never starts with a missing backup directory.
"""
# Storage directory (Docker volume)
from datetime import datetime
from pathlib import Path
from config.editable import editable
from yarl import URL # type: ignore


# ---------------------------------------------------------------------------
# Directories
# ---------------------------------------------------------------------------


DATA_DIR = Path("../data")
DATA_DIR.mkdir(exist_ok=True)

DB_FILE = DATA_DIR / "travel.db"

JOBS_DIR = Path("../data/jobs")
JOBS_DIR.mkdir(exist_ok=True)

DATA_BACKUP_DIR = DATA_DIR / "backups"
DATABASE_BACKUP_DIR = DATA_BACKUP_DIR / "db"
UPLOADS_BACKUP_DIR = DATA_BACKUP_DIR / "uploads"
FX_BACKUP_DIR = UPLOADS_BACKUP_DIR / "fx"
HEALTH_BACKUP_DIR = UPLOADS_BACKUP_DIR / "health"
WORKOUT_BACKUP_DIR = HEALTH_BACKUP_DIR / "workouts"
LOCATION_BACKUP_DIR = UPLOADS_BACKUP_DIR / "location"
LOCATION_SHORTCUTS_BACKUP_DIR = LOCATION_BACKUP_DIR / "shortcuts"
LOCATION_OVERLAND_BACKUP_DIR = LOCATION_BACKUP_DIR / "overland"
REVOLUT_BACKUP_DIR = UPLOADS_BACKUP_DIR / "revolut"
WISE_BACKUP_DIR = UPLOADS_BACKUP_DIR / "wise"

BACKUP_DIRS = [
    DATA_BACKUP_DIR,
    DATABASE_BACKUP_DIR,
    UPLOADS_BACKUP_DIR,
    FX_BACKUP_DIR,
    HEALTH_BACKUP_DIR,
    WORKOUT_BACKUP_DIR,
    LOCATION_BACKUP_DIR,
    LOCATION_SHORTCUTS_BACKUP_DIR,
    LOCATION_OVERLAND_BACKUP_DIR,
    REVOLUT_BACKUP_DIR,
    WISE_BACKUP_DIR
]

for backup_dir in BACKUP_DIRS:
    backup_dir.mkdir(exist_ok=True)

LOG_DIR = Path("./logs/")
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "server.log"
WARN_FILE = LOG_DIR / "server.warn.log"
ERROR_FILE = LOG_DIR / "server.error.log"

OVERRIDES_PATH = DATA_DIR / "config_overrides.json"

# ---------------------------------------------------------------------------
# Notifications
# ---------------------------------------------------------------------------

AVAILABLE_NOTIFICATIONS = editable("AVAILABLE_NOTIFICATIONS", "Pushcut notification Webhook URL and internal name")({
    "travelnet_test": "https://api.pushcut.io/KjvFN6-uKZjR0S3lNehts/notifications/TravelNet%20Test",
})

# ---------------------------------------------------------------------------
# Weather
# ---------------------------------------------------------------------------

STALE_DAYS = editable("STALE_DAYS", "Number of days to consider a backup stale")(7)


OPEN_METEO_URL = (URL("https://archive-api.open-meteo.com/v1/archive"))

HOURLY_VARS = editable("HOURLY_VARS", "OpenMeteo API variables that are per hour")([
    "temperature_2m",
    "apparent_temperature",
    "precipitation",
    "windspeed_10m",
    "winddirection_10m",
    "weathercode",
    "uv_index",
    "cloudcover",
    "is_day"
    ])

DAILY_VARS = editable("DAILY_VARS", "OpenMeteo API variables that are per day")([
    "sunrise",
    "sunset",
    "precipitation_sum",
    "precipitation_hours",
    "snowfall_sum",
    "wind_speed_10m_max",
    "wind_gusts_10m_max"
])


# Coordinate rounding resolution — matches Open-Meteo's ~10 km grid
COORD_PRECISION = editable("COORD_PRECISION", "Number of decimal places to round lat/lon coordinates for OpenMeteo requests")(1)

# Seconds between API requests — be a polite free-tier citizen
REQUEST_DELAY = editable("REQUEST_DELAY","Number of seconds between each OpenMeteo request.\nPolite not to spam a free service")(0.5)


# ---------------------------------------------------------------------------
# Directories
# ---------------------------------------------------------------------------

TRAVEL_START_DATE = editable("TRAVEL_START_DATE")(datetime(year=2026, month=6, day=11))
TRAVEL_START_DATE_TIMESTAMP = int(TRAVEL_START_DATE.timestamp())


# ---------------------------------------------------------------------------
# Transaction
# ---------------------------------------------------------------------------


FX_URL = URL("https://api.exchangerate.host/timeframe")
CURRENCIES = editable("CURRENCIES", "Currencies used on travel")(["GBP", "USD", "CAD", "EUR", "AUD", "NZD", "FJD", "THB", "KHR", "VND", "LAK"])
SOURCE_CURRENCY = editable("SOURCE_CURRENCY", "Currency to convert from")("GBP")

# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

INTERVAL_MINUTES = editable("INTERVAL_MINUTES", "Number of minutes between Shortcut location entries.\nAlso used for health metric aggregation")(5)

# ---------------------------------------------------------------------------
# Gap annotations
# ---------------------------------------------------------------------------

GAP_ANNOTATION_TOLERANCE_MINUTES = editable(
    "GAP_ANNOTATION_TOLERANCE_MINUTES",
    "Tolerance in minutes applied on each side of an annotation when checking\n"
    "whether it covers a detected data gap.  A value of 10 means an annotation\n"
    "needs to start at most 10 min before the gap and end at most 10 min after it."
)(10)
