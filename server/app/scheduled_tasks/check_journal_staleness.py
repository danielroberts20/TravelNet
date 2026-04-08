from config.editable import load_overrides
load_overrides()

import json
import logging
from datetime import datetime, timezone

from config.general import DATA_DIR, JOURNAL_STALENESS_HOURS
from config.logging import configure_logging
from config.settings import settings
from notifications import CronJobMailer, journal_notification, send_notification

logger = logging.getLogger(__name__)

JOURNAL_LATEST_FILE = DATA_DIR / "journal_latest.json"


def check_journal_staleness() -> dict:
    """Check whether the latest journal entry is older than JOURNAL_STALENESS_HOURS.

    Returns a dict with keys: last_entry_ts, hours_since, stale.
    Sends a push notification if stale.
    Raises RuntimeError if the file is missing or unreadable.
    """
    if not JOURNAL_LATEST_FILE.exists():
        raise RuntimeError(f"Journal latest file not found: {JOURNAL_LATEST_FILE}")

    with open(JOURNAL_LATEST_FILE) as f:
        data = json.load(f)

    raw_ts = data.get("timestamp")
    if not raw_ts:
        raise RuntimeError("Journal latest file is missing 'timestamp' field")

    last_entry = datetime.fromisoformat(raw_ts)
    if last_entry.tzinfo is None:
        last_entry = last_entry.replace(tzinfo=timezone.utc)

    now = datetime.now(timezone.utc)
    hours_since = (now - last_entry).total_seconds() / 3600
    stale = hours_since >= JOURNAL_STALENESS_HOURS

    logger.info(
        "Journal staleness check: last entry %s, %.1f hours ago, stale=%s",
        raw_ts, hours_since, stale,
    )

    if stale:
        journal_notification(
            title="📝 Journal Reminder",
            body=f"No entry in {hours_since:.0f} hours. Tap here to add an entry",
        )

    return {
        "last_entry_ts": raw_ts,
        "hours_since": round(hours_since, 1),
        "stale": stale,
    }


if __name__ == "__main__":
    configure_logging()

    check_journal_staleness()
