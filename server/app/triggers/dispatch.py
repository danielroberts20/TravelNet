from datetime import datetime, timedelta, timezone
import json
import math

from notifications import journal_notification, send_notification
from database.connection import get_conn, to_iso_str

def haversine_m(lat1, lon1, lat2, lon2) -> float:
    """Returns distance in metres between two lat/lon points."""
    R = 6_371_000  # Earth radius in metres
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))

def dispatch(trigger: str, payload: dict, cooldown_hours: int = 2, noti_title: str = None, noti_body: str = None) -> bool:
    """Returns True if notification was fired, False if suppressed by cooldown."""
    with get_conn() as conn:
        cutoff = to_iso_str(datetime.now(timezone.utc) - timedelta(hours=cooldown_hours))
        recent = conn.execute("""
            SELECT id FROM trigger_log
            WHERE trigger = ? AND fired_at >= ?
            LIMIT 1
        """, (trigger, cutoff)).fetchone()

        if recent:
            return False

        journal_notification(title=noti_title, body=noti_body, time_sensitive=False, prefix="📝 TravelNet")

        conn.execute("""
            INSERT INTO trigger_log (trigger, fired_at, payload)
            VALUES (?, ?, ?)
        """, (trigger, to_iso_str(datetime.now(timezone.utc)), json.dumps(payload)))

    return True
