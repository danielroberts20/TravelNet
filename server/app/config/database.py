from pathlib import Path


DB_DIR = Path("../data")
DB_DIR.mkdir(exist_ok=True, parents=True)
DB_FILE = DB_DIR / "travel.db"