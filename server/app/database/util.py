import sqlite3
import uuid
from datetime import datetime

from config.database import BACKUP_DIR, DB_FILE


def get_conn(read_only=False) -> sqlite3.Connection:
    """Get a new SQLite connection."""
    if read_only:
        conn = sqlite3.connect(f"file:{DB_FILE}?mode=ro", uri=True)
    else:
        conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row  # so you can access columns by name
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def backup_db(include_timestamp=False) -> str:
    if include_timestamp:
        backup_name = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4()}.db"
    else:
        backup_name = f"{uuid.uuid4()}.db"
    backup_path = BACKUP_DIR / backup_name

    source = sqlite3.connect(DB_FILE)
    dest = sqlite3.connect(backup_path)
    source.backup(dest)
    dest.close()
    source.close()
    return backup_path

def get_latest_backup() -> str:
    backup_files = sorted(BACKUP_DIR.glob("*.db"), key=lambda f: f.stat().st_mtime, reverse=True)
    if not backup_files:
        raise FileNotFoundError("No backup files found.")
    return backup_files[0]
