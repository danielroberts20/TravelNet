import math
from datetime import datetime

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
    