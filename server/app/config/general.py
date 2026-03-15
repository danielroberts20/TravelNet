# Storage directory (Docker volume)
import os
from pathlib import Path
from yarl import URL # type: ignore


DATA_DIR = Path("../data")
DATA_DIR.mkdir(exist_ok=True)

JOBS_DIR = Path("../data/jobs")
JOBS_DIR.mkdir(exist_ok=True)


DATA_BACKUP_DIR = DATA_DIR / "log_backup"
HEALTH_BACKUP_DIR = DATA_BACKUP_DIR / "health"
LOCATION_BACKUP_DIR = DATA_BACKUP_DIR / "location"
WISE_TRANSACTION_BACKUP_DIR = DATA_BACKUP_DIR / "wise"
REVOLUT_TRANSACTION_BACKUP_DIR = DATA_BACKUP_DIR / "revolut"
FX_BACKUP_DIR = DATA_BACKUP_DIR / "fx"

BACKUP_DIRS = [
    DATA_BACKUP_DIR,
    HEALTH_BACKUP_DIR,
    LOCATION_BACKUP_DIR,
    WISE_TRANSACTION_BACKUP_DIR,
    REVOLUT_TRANSACTION_BACKUP_DIR,
    FX_BACKUP_DIR
]

for backup_dir in BACKUP_DIRS:
    backup_dir.mkdir(exist_ok=True)

LOG_DIR = Path("./logs/")
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "server.log"
WARN_FILE = LOG_DIR / "server.warn.log"
ERROR_FILE = LOG_DIR / "server.error.log"

FX_URL = URL("https://api.exchangerate.host/historical")
CURRENCIES = ["GBP", "USD", "CAN", "AUD", "NZD", "FJD", "THB", "KHR", "VND", "LAK"]
SOURCE_CURRENCY = "GBP"

WISE_SOURCE_MAP = {
    "137103728_USD": "🇺🇸 USD",
    "137103780_AUD": "🇦🇺 AUD",
    "137103867_CAD": "🇨🇦 CAD",
    "138167086_AUD": "🇦🇺 Melbourne Fund",
    "148241577_USD": "🐲 South East Asia",
    "137103719_GBP": "🇬🇧 GBP",
    "138167566_NZD": "🇳🇿 NZD",
    "140828771_USD": "🇺🇸 US Travel",
    "147924418_EUR": "🇪🇺 EUR",
    "148241731_NZD": "🇳🇿 New Zealand Travel"
}

UPLOAD_TOKEN = os.getenv("UPLOAD_TOKEN", None)
FX_API_KEY = os.getenv("FX_API_KEY", None)
OVERLAND_TOKEN = os.getenv("OVERLAND_TOKEN", None)
INTERVAL_MINUTES = 5
METRIC_AGGREGATION = {
    "Active Energy": {"Active Energy (kJ)": "sum"},
    "Apple Exercise Time": {"Apple Exercise Time (min)": "sum"},
    "Apple Stand Hour": {"Apple Stand Hour (count)": "sum"},
    "Apple Stand Time": {"Apple Stand Time (min)": "sum"},
    "Blood Oxygen Saturation": {"Blood Oxygen Saturation (%)": "mean"},
    "Environmental Audio Exposure": {"Environmental Audio Exposure (dBASPL)": "mean"},
    "Flights Climbed": {"Flights Climbed (count)": "sum"},
    "Heart Rate Variability": {"Heart Rate Variability (ms)": "mean"},
    "Heart Rate": {"Min (count/min)": "min", "Avg (count/min)": "mean", "Max (count/min)": "max"},
    "Physical Effort": {"Physical Effort (kcal/hr·kg)": "sum"},
    "Resting Energy": {"Resting Energy (kJ)": "sum"},
    "Resting Heart Rate": {"Resting Heart Rate (count/min)": "mean"},
    "Sleep Analysis": {"Sleep Analysis (min)": "sum"},
    "Stair Speed: Down": {"Stair Speed: Down (m/s)": "mean"},
    "Stair Speed: Up": {"Stair Speed: Up (m/s)": "mean"},
    "Step Count": {"Step Count (count)": "sum"},
    "Walking + Running Distance": {"Walking + Running Distance (km)": "sum"},
    "Walking Asymmetry Percentage": {"Walking Asymmetry Percentage (%)": "mean"},
    "Walking Double Support Percentage": {"Walking Double Support Percentage (%)": "mean"},
    "Walking Heart Rate Average": {"Walking Heart Rate Average (count/min)": "mean"},
    "Walking Speed": {"Walking Speed (km/hr)": "mean"},
    "Walking Step Length": {"Walking Step Length (cm)": "mean"},
}

METRICS = [
    "Active Energy",
    "Alcohol Consumption",
    "Apple Exercise Time",
    "Apple Move Time",
    "Apple Sleeping Wrist Temperature",
    "Apple Stand Hour",
    "Apple Stand Time",
    "Atrial Fibrillation Burden",
    "Basal Body Temperature",
    "Biotin",
    "Blood Alcohol Content",
    "Blood Glu",
    "Blood Oxygen Saturation",
    "Blood Pres",
    "Body Fat Percentage",
    "Body Mass Index",
    "Breathing Disturbances",
    "Caffeine",
    "Calcium",
    "Carbohydrates",
    "Cardio Recovery",
    "Chloride",
    "Cholesterol",
    "Chromium",
    "Copper",
    "Cycling Cadence",
    "Cycling Distance",
    "Cycling Functional Threshold Power",
    "Cycling Power",
    "Cycling Speed",
    "Dietary Energy",
    "Distance Downhill Snow Sports",
    "Electrodermal Activity",
    "Environmental Audio Exposure",
    "Fiber",
    "Flights Climbed",
    "Fola",
    "Forced Expiratory Volume 1",
    "Forced Vital Capacity",
    "Handwashing",
    "Headphone Audio Exposure",
    "Heart Rate Variability",
    "Height",
    "Inhaler Usage",
    "Insulin Delivery",
    "Iodine",
    "Iron",
    "Lean Body Mas",
    "Magnesium",
    "Manganese",
    "Mindful Minutes",
    "Molybdenum",
    "Monounsaturated Fat",
    "Niacin",
    "Number of Times Fallen",
    "Pantothenic Acid",
    "Peak Expiratory Flow Rate",
    "Peripheral Perfusion Index",
    "Phosphorus",
    "Physical Effort",
    "Polyunsaturated Fat",
    "Potassium",
    "Protein",
    "Push Count",
    "Respiratory Rate",
    "Resting Energy",
    "Resting Heart Rate",
    "Riboflavin",
    "Running Ground Contact Time",
    "Running Power",
    "Running Speed",
    "Running Stride Length",
    "Running Vertical Oscillation",
    "Saturated Fat",
    "Selenium",
    "Sexual Activity",
    "Six-Minute Walking Test Distance",
    "Sleep Analysis",
    "Sodium",
    "Stair Speed: Down",
    "Stair Speed: Up",
    "Step Count",
    "Sugar",
    "Swimming Distance",
    "Swimming Stroke Count",
    "Thiamin",
    "Time in Daylight",
    "Toothbrushing",
    "Total Fat",
    "UV Exposure",
    "Underwater Depth",
    "Underwater Temperature",
    "VO2 Max",
    "Vitamin A",
    "Vitamin B12",
    "Vitamin B6",
    "Vitamin C",
    "Vitamin D",
    "Vitamin E",
    "Vitamin K",
    "Waist Circumference",
    "Walking + Running Distance",
    "Walking Asymmetry Percentage",
    "Walking Double Support Percentage",
    "Walking Heart Rate Average",
    "Walking Speed",
    "Walking Step Length",
    "Water",
    "Weight",
    "Wheelchair Distance",
    "Zinc"
]
