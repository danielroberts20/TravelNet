from datetime import datetime, timedelta, timezone
import json
import math

from notifications import label_known_place_notification
from database.connection import get_conn, to_iso_str
from database.triggers.table import table as trigger_table, TriggerRecord

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

    label_known_place_notification(title=noti_title, body=noti_body, time_sensitive=False)

    trigger_table.insert(TriggerRecord(
        trigger=trigger,
        fired_at=to_iso_str(datetime.now(timezone.utc)),
        payload=json.dumps(payload),
    ))

    return True
