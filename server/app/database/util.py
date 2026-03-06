import csv
import logging
import os
import sqlite3
import uuid
from datetime import datetime

from config.database import BACKUP_DIR, DB_FILE
from config.general import FX_BACKUP_DIR, HEALTH_BACKUP_DIR, LOCATION_BACKUP_DIR
from database.integration import insert_log
from telemetry_models import Log

logger = logging.getLogger(__name__)

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

def rebuild_db():
    from database.integration import init_db
    from uploads.utils import input_csv
    from database.exchange.util import insert_fx_file
    
    logger.info("Rebuilding database from CSV logs...")
    with get_conn() as conn:
        conn.execute("DROP TABLE IF EXISTS cellular_states;")
        conn.execute("DROP TABLE IF EXISTS locations;")
        conn.execute("DROP TABLE IF EXISTS health_data;")
        conn.execute("DROP TABLE IF EXISTS health_sources;")
        conn.execute("DROP TABLE IF EXISTS fx_rates;")
        conn.commit()   

    init_db()

    for f in sorted(os.listdir(HEALTH_BACKUP_DIR)):
        if f.endswith(".csv"):
            input_csv(open(HEALTH_BACKUP_DIR / f, "r"))
    
    for f in sorted(os.listdir(LOCATION_BACKUP_DIR)):
        if f.endswith(".csv"):
            reader = csv.DictReader(LOCATION_BACKUP_DIR / f)
            for idx, row in enumerate(reader):
                try:
                    log = Log.from_strings(**row)
                    insert_log(log)
                    inserted += 1
                except Exception as e:
                    # Skip bad rows
                    logger.warning(f"Bad row on line {idx+2}.\t CSV entry: {row}\tException: {e}")
                    continue
    
        
    for f in sorted(os.listdir(FX_BACKUP_DIR)):
        if f.endswith(".json"):
            insert_fx_file(FX_BACKUP_DIR / f)
    
    logger.info("Finished rebuilding database")
