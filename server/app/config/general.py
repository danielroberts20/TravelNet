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
from datetime import datetime
from pathlib import Path
from config.editable import editable
from yarl import URL # type: ignore


# ---------------------------------------------------------------------------
# Storage & Backups
# ---------------------------------------------------------------------------

DATA_DIR = Path("/data")

DB_FILE = DATA_DIR / "travel.db"

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

# DATA_DIR is a Docker volume mount — only create subdirs when running inside Docker.
# Outside Docker (e.g. during tests), /data won't exist and we skip the mkdir calls
# to avoid creating stray directories relative to the process working directory.
if DATA_DIR.exists():
    for backup_dir in BACKUP_DIRS:
        backup_dir.mkdir(parents=True, exist_ok=True)

STALE_DAYS = editable("STALE_DAYS", "Number of days to consider a backup stale", group="Storage & Backups")(7)
PAGE_SIZE = editable("PAGE_SIZE", "Number of records to fetch at a time", group="Storage & Backups")(2000)


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

LOG_DIR = Path("./logs/")
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "server.log"
WARN_FILE = LOG_DIR / "server.warn.log"
ERROR_FILE = LOG_DIR / "server.error.log"

DAILY_CRON_JOBS = [
    "geocode_places",
    "get_fx",
    "backfill_place",
    #"detect_timezone_transitions",
    #"detect_country_transitions",
    "push_public_stats",
    #"compute_daily_summary",
]


# ---------------------------------------------------------------------------
# Runtime Configuration
# ---------------------------------------------------------------------------

OVERRIDES_PATH = DATA_DIR / "config_overrides.json"

TRAVEL_YML = Path("/travel.yml")


# ---------------------------------------------------------------------------
# Flight Detection
# ---------------------------------------------------------------------------
FLIGHT_GAP_MIN_HOURS = editable("FLIGHT_GAP_MIN_HOURS", description="Minimum gap duration (hours) between location points to be considered a possible flight.", group="Flight Detection")(2)
FLIGHT_DISTANCE_MIN_KM = editable("FLIGHT_DISTANCE_MIN_KM", description="Minimum great-circle distance (km) across a gap to be considered a possible flight.", group="Flight Detection")(200)

# ---------------------------------------------------------------------------
# Notifications
# ---------------------------------------------------------------------------

AVAILABLE_NOTIFICATIONS = editable("AVAILABLE_NOTIFICATIONS", "Pushcut notification Webhook URL and internal name", group="Notifications")({
    "travelnet_test": "https://api.pushcut.io/KjvFN6-uKZjR0S3lNehts/notifications/TravelNet%20Test",
})


# ---------------------------------------------------------------------------
# API & Access Control
# ---------------------------------------------------------------------------

PUBLIC_ALLOWED_PREFIXES = ["/public/"]


# ---------------------------------------------------------------------------
# Travel Itinerary
# ---------------------------------------------------------------------------

COUNTRY_DEPARTURE_DATES = editable("COUNTRY_DEPARTURE_DATES", "Dates", group="Travel Itinerary")({
    "UK": (datetime(year=2026, month=6, day=11)),
    "USA": datetime(year=2026, month=9, day=2)
})

TRAVEL_START_DATE = COUNTRY_DEPARTURE_DATES.get("UK")
TRAVEL_START_DATE_TIMESTAMP = int(TRAVEL_START_DATE.timestamp())


def _refresh_derived():
    """Recompute constants derived from editable values. Called by load_overrides() after patching."""
    import config.general as _m
    uk_date = _m.COUNTRY_DEPARTURE_DATES.get("UK")
    if isinstance(uk_date, datetime):
        _m.TRAVEL_START_DATE = uk_date
        _m.TRAVEL_START_DATE_TIMESTAMP = int(uk_date.timestamp())


# ---------------------------------------------------------------------------
# Location
# ---------------------------------------------------------------------------

LOCATION_CHANGE_RADIUS_M = editable("LOCATION_CHANGE_RADIUS_M", "Distance in meters to consider a location change", group="Location")(500)
LOCATION_STAY_DURATION_MINS = editable("LOCATION_STAY_DURATION_MINS", "Duration in minutes of stationary GPS required to confirm a new (previously unseen) location", group="Location")(30)
LOCATION_REVISIT_DURATION_MINS = editable("LOCATION_REVISIT_DURATION_MINS", "Duration in minutes of stationary GPS required to confirm a return visit to a known location (shorter than LOCATION_STAY_DURATION_MINS)", group="Location")(5)
LOCATION_DEPARTURE_CONFIRMATION_MINS = editable("LOCATION_DEPARTURE_CONFIRMATION_MINS", "Minutes since the last in-radius point required to confirm a departure (applies to all place types)", group="Location")(5)
LOCATION_MINIMUM_POINTS = editable("LOCATION_MINIMUM_POINTS", "Minimum number of location points within the stay duration to consider it a valid stay", group="Location")(5)
LOCATION_STATIONARITY_RADIUS_M = editable("LOCATION_STATIONARITY_RADIUS_M", "Distance in meters that location points must be within to be considered stationary rather than in transit", group="Location")(150)
LOCATION_STREAK_POINT_LIMIT = editable("LOCATION_STREAK_POINT_LIMIT", "Maximum number of recent location points to scan when computing the current stationary streak", group="Location")(1000)
LOCATION_NOISE_ACCURACY_THRESHOLD = editable("LOCATION_NOISE_ACCURACY_THRESHOLD", "Threshold for horizontal accuracy to consider a location point as tier 1 noise", group="Location")(100)
TIER2_TRAILING_SKIP = editable("TIER2_TRAILING_SKIP", "Number of most recent points to skip when applying tier 2 noise flags, due to incomplete next-point context", group="Location")(10)
TIER2_DISPLACEMENT_M = editable("TIER2_DISPLACEMENT_M", "Distance in meters that a point must displace from its predecessor to be considered a potential noise spike in tier 2 detection", group="Location")(150)
TIER2_RETURN_M = editable("TIER2_RETURN_M", "Distance in meters that the point following a potential tier 2 spike must return to be considered an out-and-back noise signature", group="Location")(150)
TIER2_WINDOW_S = editable("TIER2_WINDOW_S", "Time window in seconds to look for the next point when applying tier 2 noise flags", group="Location")(30)

LOCATION_TIME_WINDOW = editable(
    "LOCATION_TIME_WINDOW",
    "Seconds — match window between Overland and Shortcuts points when deduplicating",
    group="Location"
)(60)

LOCATION_DIST_THRESHOLD = editable(
    "LOCATION_DIST_THRESHOLD",
    "Degrees — distance below which two points are considered the same location (~1 km)",
    group="Location"
)(0.01)

GAP_ANNOTATION_TOLERANCE_MINUTES = editable(
    "GAP_ANNOTATION_TOLERANCE_MINUTES",
    "Tolerance in minutes applied on each side of an annotation when checking\n"
    "whether it covers a detected data gap.  A value of 10 means an annotation\n"
    "needs to start at most 10 min before the gap and end at most 10 min after it.",
    group="Location"
)(10)

DWELL_MIN_POINTS = editable(key="DWELL_MIN_POINTS", 
                            description="Minimum consecutive GPS points in a new country before registering a country transition. " \
                                        "Higher values filter out more border noise.", group="Location")(3)


# ---------------------------------------------------------------------------
# Weather
# ---------------------------------------------------------------------------

OPEN_METEO_URL = (URL("https://archive-api.open-meteo.com/v1/archive"))

HOURLY_VARS = editable("HOURLY_VARS", "OpenMeteo API variables that are per hour", group="Weather")([
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

DAILY_VARS = editable("DAILY_VARS", "OpenMeteo API variables that are per day", group="Weather")([
    "sunrise",
    "sunset",
    "precipitation_sum",
    "precipitation_hours",
    "snowfall_sum",
    "wind_speed_10m_max",
    "wind_gusts_10m_max"
])

# Coordinate rounding resolution — matches Open-Meteo's ~10 km grid
COORD_PRECISION = editable("COORD_PRECISION", "Number of decimal places to round lat/lon coordinates for OpenMeteo requests", group="Weather")(1)

# Seconds between API requests — be a polite free-tier citizen
REQUEST_DELAY = editable("REQUEST_DELAY","Number of seconds between each OpenMeteo request.\nPolite not to spam a free service", group="Weather")(0.5)


# ---------------------------------------------------------------------------
# Foreign Exchange
# ---------------------------------------------------------------------------

FX_BASE_URL = URL("https://api.exchangerate.host")
FX_TIMEFRAME_URL = FX_BASE_URL / "timeframe"
FX_DATE_URL = FX_BASE_URL / "historical"
CURRENCIES = editable("CURRENCIES", "Currencies used on travel", group="Foreign Exchange")(["GBP", "USD", "CAD", "EUR", "AUD", "NZD", "FJD", "THB", "KHR", "VND", "LAK"])
SOURCE_CURRENCY = editable("SOURCE_CURRENCY", "Currency to convert from", group="Foreign Exchange")("GBP")


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

INTERVAL_MINUTES = editable("INTERVAL_MINUTES", "Number of minutes between Shortcut location entries.\nAlso used for health metric aggregation", group="Health")(5)

HEALTH_GAP_LOOKBACK_DAYS = editable("HEALTH_GAP_LOOKBACK_DAYS", "Days of history to analyse for gap detection", group="Health")(30)

HEALTH_GAP_MULTIPLIER = editable("HEALTH_GAP_MULTIPLIER", "Gap must exceed N× median cadence to be flagged", group="Health")(3.0)

HEALTH_GAP_MIN_HOURS = editable("HEALTH_GAP_MIN_HOURS", "Absolute floor (hours) — gaps shorter than this are never flagged", group="Health")(10.0)

HEALTH_MIN_HISTORY_POINTS = editable("HEALTH_MIN_HISTORY_POINTS", "Minimum points needed to compute a reliable median cadence", group="Health")(10)

HEALTH_MIN_POINTS_AFTER = editable("HEALTH_MIN_POINTS_AFTER", "Fewer than this many records after a gap → marked tentative", group="Health")(5)

# ---------------------------------------------------------------------------
# Journal
# ---------------------------------------------------------------------------

JOURNAL_STALENESS_HOURS = editable("JOURNAL_STALENESS_HOURS", "Number of hours after which a journal entry is considered stale", group="Journal")(18)

# ---------------------------------------------------------------------------
# Daily Summary
# ---------------------------------------------------------------------------

RECOMPUTE_WINDOW_DAYS = editable("RECOMPUTE_WINDOW_DAYS", "How many past days to scan for rows needing recompute", group="Daily Summary")(14)
WAKING_HOURS = editable("WAKING_HOURS", "Typical hours you are awake and active. 18 would be 06:00 local → local midnight", group="Daily Summary")(18)
EXPECTED_POINTS_PER_HOUR = editable("EXPECTED_POINTS_PER_HOUR", "Expected Overland points at typical cadence during waking hours.", group="Daily Summary")(60)
BACKFILL_MONTHS = editable("BACKFILL_MONTHS", "Number of months to backfill for transaction data", group="Daily Summary")(2)
BACKFILL_DAYS = editable("BACKFILL_DAYS", "Number of days to backfill for weather data. Must exceed the 40-day weather fetch window", group="Daily Summary")(45)