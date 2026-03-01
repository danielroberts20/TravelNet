# Storage directory (Docker volume)
import os


from pathlib import Path


DATA_DIR = Path("../data")
DATA_DIR.mkdir(exist_ok=True)

JOBS_DIR = Path("../data/jobs")
JOBS_DIR.mkdir(exist_ok=True)

# Optional auth token
UPLOAD_TOKEN = os.getenv("UPLOAD_TOKEN", None)
INTERVAL_MINUTES = 10
METRIC_AGGREGATION={
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
