import sqlite3
import uuid

from config.database import DB_DIR, DB_FILE


def get_conn(read_only=False) -> sqlite3.Connection:
    """Get a new SQLite connection."""
    if read_only:
        conn = sqlite3.connect(f"file:{DB_FILE}?mode=ro", uri=True)
    else:
        conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row  # so you can access columns by name
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def backup_db():
    backup_name = f"{uuid.uuid4()}.db"
    backup_path = DB_DIR / backup_name

    source = sqlite3.connect(DB_FILE)
    dest = sqlite3.connect(backup_path)
    source.backup(dest)
    dest.close()
    source.close()
    return backup_path
