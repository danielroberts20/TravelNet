import math
from datetime import datetime, timezone


def time_ago(iso_timestamp: str | None) -> str | None:
    """Convert an ISO 8601 UTC timestamp to a human-readable 'X ago' string."""
    if iso_timestamp is None:
        return None
    dt = datetime.fromisoformat(iso_timestamp.replace("Z", "+00:00"))
    delta = datetime.now(timezone.utc) - dt
    total_seconds = int(delta.total_seconds())
    if total_seconds < 0:
        return "just now"
    if total_seconds < 3600:
        return f"{total_seconds // 60}m ago"
    if total_seconds < 86400:
        h = total_seconds // 3600
        m = (total_seconds % 3600) // 60
        return f"{h}h {m}m ago" if m else f"{h}h ago"
    return f"{total_seconds // 86400}d ago"


def _haversine(lat1, lon1, lat2, lon2, r):
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))

def haversine_m(lat1, lon1, lat2, lon2) -> float:
    """Returns distance in metres between two lat/lon points."""
    return _haversine(lat1, lon1, lat2, lon2, 6_371_000)

def haversine_km(lat1, lon1, lat2, lon2) -> float:
    """Returns distance in kilometres between two lat/lon points."""
    return _haversine(lat1, lon1, lat2, lon2, 6_371)

def parse_ts(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00"))
    