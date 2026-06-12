from config.editable import load_overrides
load_overrides()

import json
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

from prefect import task, flow
from prefect.logging import get_run_logger

from config.general import DATA_DIR, JOURNAL_REMIND_EVENING_HOUR, JOURNAL_REMIND_MORNING_HOUR
from database.connection import get_conn
from notifications import journal_notification, record_flow_result, notify_on_completion, log_on_success

JOURNAL_LATEST_FILE = DATA_DIR / "journal_latest.json"

TRIGGER_EVENING = "journal_reminder_evening"
TRIGGER_MORNING = "journal_reminder_morning"


@task
def get_local_timezone() -> ZoneInfo:
    """Return the most recent known timezone from transition_timezone, defaulting to UTC."""
    logger = get_run_logger()
    with get_conn(read_only=True) as conn:
        row = conn.execute(
            "SELECT to_tz FROM transition_timezone ORDER BY transitioned_at DESC LIMIT 1"
        ).fetchone()
    tz_name = row[0] if row else "UTC"
    logger.info("Local timezone: %s", tz_name)
    return ZoneInfo(tz_name)


@task
def get_last_journal_ts() -> datetime:
    """Return the last journal entry timestamp from journal_latest.json.

    Raises RuntimeError if the file is missing, unreadable, or has no timestamp field.
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
    return last_entry


@task
def already_fired_today(trigger: str, today_midnight_utc: datetime) -> bool:
    """Return True if this trigger has already fired since today's local midnight."""
    cutoff = today_midnight_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    with get_conn(read_only=True) as conn:
        row = conn.execute(
            "SELECT 1 FROM trigger_log WHERE trigger = ? AND fired_at >= ? LIMIT 1",
            (trigger, cutoff),
        ).fetchone()
    return row is not None


@task
def log_trigger(trigger: str) -> None:
    """Write a dedup row to trigger_log so this reminder isn't re-sent today."""
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO trigger_log (trigger, fired_at) VALUES (?, ?)",
            (trigger, now_utc),
        )


def _run(window: str = "evening") -> dict:
    """Two-window journal reminder flow.

    window="evening"  — 20:00 local: remind if no entry written yet today.
    window="morning"  — 09:00 local: catch-up nudge if nothing written since yesterday midnight.
    """
    logger = get_run_logger()

    tz = get_local_timezone()
    now_local = datetime.now(tz)
    today_midnight_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    today_midnight_utc = today_midnight_local.astimezone(timezone.utc)

    last_ts = get_last_journal_ts()
    logger.info("Last journal entry: %s", last_ts.isoformat())

    if window == "evening":
        trigger = TRIGGER_EVENING
        written = last_ts >= today_midnight_utc
        title = "📝 Journal Reminder"
        body = "No entry yet today — write one tonight"

    elif window == "morning":
        trigger = TRIGGER_MORNING
        yesterday_midnight_utc = today_midnight_utc - timedelta(days=1)
        written = last_ts >= yesterday_midnight_utc
        title = "📝 Journal Catch-up"
        body = "Still no entry for yesterday — write it while it's fresh"

    else:
        raise ValueError(f"Unknown window: {window!r}. Expected 'evening' or 'morning'.")

    hours_since = (datetime.now(timezone.utc) - last_ts).total_seconds() / 3600
    logger.info("[%s] last_entry=%s, %.1f hours ago, written=%s", window, last_ts.isoformat(), hours_since, written)

    if written:
        logger.info("[%s] Entry found — no reminder needed", window)
        result = {"window": window, "reminded": False, "last_entry_ts": last_ts.isoformat(), "hours_since": round(hours_since, 1)}
    elif already_fired_today(trigger, today_midnight_utc):
        logger.info("[%s] Already fired today — skipping", window)
        result = {"window": window, "reminded": False, "skipped": "already_fired", "last_entry_ts": last_ts.isoformat()}
    else:
        logger.info("[%s] No entry found — sending reminder", window)
        journal_notification(title=title, body=body, retro=window == "morning")
        log_trigger(trigger)
        result = {"window": window, "reminded": True, "last_entry_ts": last_ts.isoformat(), "hours_since": round(hours_since, 1)}

    record_flow_result(result)
    return result


@flow(name="Check Journal Staleness Evening", on_failure=[notify_on_completion], on_completion=[log_on_success])
def check_journal_staleness_evening_flow() -> dict:
    return _run(window="evening")

@flow(name="Check Journal Staleness Morning", on_failure=[notify_on_completion], on_completion=[log_on_success])
def check_journal_staleness_morning_flow() -> dict:
    return _run(window="morning")