from pathlib import Path

from config.general import DATA_DIR


DB_FILE = DATA_DIR / "travel.db"

BACKUP_DIR = DATA_DIR / "database_backup"
BACKUP_DIR.mkdir(exist_ok=True)