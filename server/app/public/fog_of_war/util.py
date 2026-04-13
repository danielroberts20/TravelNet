import math
import random
import sqlite3
from datetime import datetime, timezone
from database.connection import get_conn


def _fuzz_coordinate(lat: float, lon: float, sigma_m: float = 500.0) -> tuple[float, float]:
    """Apply Gaussian noise in metres, converted to degrees."""
    delta_lat = random.gauss(0, sigma_m) / 111_320
    delta_lon = random.gauss(0, sigma_m) / (111_320 * math.cos(math.radians(lat)))
    return lat + delta_lat, lon + delta_lon


def _truncate_to_hour(timestamp_str: str) -> str:
    """Truncate ISO 8601 timestamp string to nearest hour."""
    dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
    truncated = dt.replace(minute=0, second=0, microsecond=0)
    return truncated.strftime("%Y-%m-%dT%H:00:00Z")