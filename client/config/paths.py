# client/config/paths.py
from pathlib import Path

BASE = Path(__file__).parent.parent
DB_PATH = BASE / "data" / "raw" / "travel.db"
PROCESSED_DIR = BASE / "data" / "processed"
MODELS_DIR = BASE / "models"
OUTPUTS_DIR = BASE / "outputs"

# Make sure directories exist
for d in [PROCESSED_DIR, MODELS_DIR, OUTPUTS_DIR]:
    d.mkdir(parents=True, exist_ok=True)