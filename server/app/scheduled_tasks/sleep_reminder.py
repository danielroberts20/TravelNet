"""
sleep_reminder.py
─────────────────
Two-flow system for a personalised watch-to-sleep reminder.

Flow 1 (sleep-reminder-schedule): runs daily at 12:00 local time.
  - Queries health_sleep for the last N nights
  - Converts each onset to local time using transition_timezone
  - Computes a linearly-weighted median (recent nights weighted more)
  - Subtracts lead_minutes
  - Creates a Prefect-scheduled run of Flow 2 at the result

Flow 2 (sleep-reminder-notify): no cron; only ever created by Flow 1.
  - Fires at the scheduled time
  - Sends an immediate Pushcut notification

Idempotency: a lock file at /data/.sleep_reminder_date stores today's local
date. If the scheduler sees today's date already written, it exits early.
This prevents double-scheduling if the flow is manually re-triggered.
"""

import os
from datetime import datetime, date, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

from prefect import flow, task, get_run_logger
from prefect.client.orchestration import get_client
from prefect.states import Scheduled

from config.editable import load_overrides
load_overrides()

import config.general as cfg
from config.settings import settings
from database.connection import get_conn
from notifications import notify_on_completion, send_notification


REMINDER_LOCK_FILE = "/data/.sleep_reminder_date"
NOTIFY_DEPLOYMENT_NAME = "Send Sleep Reminder"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _weighted_median(values: list[float], weights: list[float]) -> float:
    """
    Weighted median: the value at which cumulative weight crosses 50%.
    Values and weights must be the same length.
    """
    paired = sorted(zip(values, weights))
    total = sum(weights)
    cumulative = 0.0
    for v, w in paired:
        cumulative += w
        if cumulative >= total / 2:
            return v
    return paired[-1][0]


def _minutes_to_hhmm(minutes: float) -> str:
    """Convert minutes-since-midnight to HH:MM string (handles >1440)."""
    mins_norm = int(minutes) % (24 * 60)
    return f"{mins_norm // 60:02d}:{mins_norm % 60:02d}"


# ── Tasks ─────────────────────────────────────────────────────────────────────

@task
def get_current_timezone() -> str:
    logger = get_run_logger()
    with get_conn(read_only=True) as conn:
        row = conn.execute(
            """
            SELECT to_tz FROM transition_timezone
            ORDER BY transitioned_at DESC
            LIMIT 1
            """
        ).fetchone()
    tz = row["to_tz"] if row else "UTC"
    logger.info("Current timezone: %s", tz)
    return tz


@task
def check_already_scheduled(tz_name: str) -> bool:
    """
    Return True if the reminder was already scheduled today (idempotency guard).
    Reads /data/.sleep_reminder_date which contains the last scheduled local date.
    """
    tz = ZoneInfo(tz_name)
    today_str = datetime.now(tz).date().isoformat()
    try:
        with open(REMINDER_LOCK_FILE) as f:
            return f.read().strip() == today_str
    except FileNotFoundError:
        return False


@task
def get_sleep_onsets(lookback_days: int, tz_name: str) -> list[float]:
    """
    Query health_sleep for the last `lookback_days` nights.
    Returns a list of sleep onset times as minutes-since-midnight
    in local time, ordered oldest-first.

    Sleep onset = first non-"In Bed", non-"Awake" stage per night.

    Night attribution: a "night" spans from local noon on day D-1 to
    local noon on day D. This means 23:30 on May 3 and 01:30 on May 4
    both attribute to the night of May 3. Achieved by subtracting 12h
    before taking the date.

    Times between midnight and noon are stored as minutes > 1440
    (e.g. 01:30 = 1530 minutes) so that the median treats them as
    "late" rather than "early morning". The caller normalises before
    scheduling.
    """
    logger = get_run_logger()
    tz = ZoneInfo(tz_name)
    buffer_days = lookback_days + 2  # extra buffer for attribution window

    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT start_ts, stage
            FROM health_sleep
            WHERE stage NOT IN ('In Bed', 'Awake')
              AND start_ts >= datetime('now', ?)
            ORDER BY start_ts ASC
            """,
            (f"-{buffer_days} days",)
        ).fetchall()

    if not rows:
        logger.warning("No sleep data found in lookback window")
        return []

    # Find earliest onset per night
    nights: dict[date, datetime] = {}

    for row in rows:
        raw = row["start_ts"]          # ← was row["start_date"]
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        start_utc = datetime.fromisoformat(raw)

        start_local = start_utc.astimezone(tz)
        # Noon-to-noon attribution: subtract 12h, then take date
        night_date = (start_local - timedelta(hours=12)).date()

        if night_date not in nights or start_utc < nights[night_date]:
            nights[night_date] = start_utc

    # Filter: keep only nights within the lookback window, exclude today
    today_local = datetime.now(tz).date()
    cutoff = today_local - timedelta(days=lookback_days)
    valid = {d: dt for d, dt in nights.items() if cutoff <= d < today_local}

    if not valid:
        logger.warning("No complete nights found after filtering")
        return []

    # Convert to minutes-since-midnight, oldest first
    onset_minutes: list[float] = []
    for _, onset_utc in sorted(valid.items()):
        onset_local = onset_utc.astimezone(tz)
        mins = onset_local.hour * 60 + onset_local.minute + onset_local.second / 60
        # Normalise: treat anything before noon as "next day late night"
        if mins < 12 * 60:
            mins += 24 * 60
        onset_minutes.append(mins)

    logger.info(
        f"Sleep onset data: {len(onset_minutes)} nights. "
        f"Range: {_minutes_to_hhmm(min(onset_minutes))} – "
        f"{_minutes_to_hhmm(max(onset_minutes))}"
    )
    return onset_minutes


@task
def compute_reminder_time(
    onset_minutes: list[float],
    tz_name: str,
    lead_minutes: int,
    fallback_time: str,
    min_data_nights: int,
) -> datetime:
    """
    Compute tonight's reminder as a timezone-aware datetime.

    If sufficient data exists, uses a linearly-weighted median of onset
    times (most recent night has the highest weight). Otherwise uses the
    configured fallback time.

    Returns reminder_dt = predicted_onset - lead_minutes, always in
    the future. If the result has already passed today, schedules for
    tomorrow (edge case if the flow runs very late).
    """
    logger = get_run_logger()
    tz = ZoneInfo(tz_name)

    if len(onset_minutes) < min_data_nights:
        logger.warning(
            f"Insufficient data ({len(onset_minutes)} nights < {min_data_nights} minimum). "
            f"Using fallback: {fallback_time}"
        )
        h, m = map(int, fallback_time.split(":"))
        predicted = h * 60 + m
        # Normalise fallback to "late night" representation
        if predicted < 12 * 60:
            predicted += 24 * 60
        source = "fallback"
    else:
        n = len(onset_minutes)
        weights = list(range(1, n + 1))  # 1 = oldest, N = most recent
        predicted = _weighted_median(onset_minutes, weights)
        source = f"weighted median of {n} nights"

    reminder_mins = predicted - lead_minutes
    logger.info(
        f"Predicted onset: {_minutes_to_hhmm(predicted)} ({source}). "
        f"Reminder at: {_minutes_to_hhmm(reminder_mins)} "
        f"({lead_minutes}min lead)"
    )

    # Build the scheduled datetime in local time
    now_local = datetime.now(tz)
    today = now_local.date()

    reminder_norm = int(reminder_mins) % (24 * 60)
    r_hour = reminder_norm // 60
    r_minute = reminder_norm % 60
    # If reminder_mins >= 1440, it falls after midnight → tomorrow's date
    target_date = today + timedelta(days=1) if reminder_mins >= 24 * 60 else today

    reminder_dt = datetime(
        target_date.year, target_date.month, target_date.day,
        r_hour, r_minute, 0,
        tzinfo=tz,
    )

    # Safety: if already in the past (flow ran very late), push to tomorrow
    if reminder_dt <= now_local:
        reminder_dt += timedelta(days=1)
        logger.warning("Computed reminder was in the past — rescheduled to tomorrow")

    logger.info(f"Final reminder datetime: {reminder_dt.isoformat()}")
    return reminder_dt


@task
async def schedule_notify_flow(reminder_dt: datetime) -> None:
    """
    Create a Prefect flow run for sleep-reminder-notify, scheduled at reminder_dt.
    The notify deployment must already be registered (no cron, on-demand only).
    """
    logger = get_run_logger()
    async with get_client() as client:
        deployments = await client.read_deployments()
        target = next(
            (d for d in deployments if d.name == NOTIFY_DEPLOYMENT_NAME),
            None,
        )
        if target is None:
            raise RuntimeError(
                f"Deployment '{NOTIFY_DEPLOYMENT_NAME}' not found. "
                "Ensure it is registered in deployments.py."
            )
        await client.create_flow_run_from_deployment(
            deployment_id=target.id,
            state=Scheduled(scheduled_time=reminder_dt),
        )
    logger.info(f"Notify flow run created, scheduled for {reminder_dt.isoformat()}")


@task
def write_lock_file(tz_name: str) -> None:
    """Write today's local date to the lock file to prevent double-scheduling."""
    tz = ZoneInfo(tz_name)
    today_str = datetime.now(tz).date().isoformat()
    with open(REMINDER_LOCK_FILE, "w") as f:
        f.write(today_str)


# ── Flow 1: Scheduler ─────────────────────────────────────────────────────────

@flow(name="Schedule Sleep Reminder", on_failure=[notify_on_completion])
async def sleep_reminder_schedule_flow():
    """
    Runs daily at noon local time.
    Predicts tonight's sleep onset and schedules the notify flow.
    """
    logger = get_run_logger()

    lookback_days = cfg.SLEEP_REMINDER_LOOKBACK_DAYS
    lead_minutes = cfg.SLEEP_REMINDER_LEAD_MINUTES
    fallback_time = cfg.SLEEP_REMINDER_FALLBACK_TIME
    min_data_nights = cfg.SLEEP_REMINDER_MIN_DATA_NIGHTS

    tz_name = get_current_timezone()

    if check_already_scheduled(tz_name):
        logger.info("Reminder already scheduled today — skipping (idempotency guard)")
        return

    onset_minutes = get_sleep_onsets(lookback_days, tz_name)
    reminder_dt = compute_reminder_time(
        onset_minutes, tz_name, lead_minutes, fallback_time, min_data_nights
    )
    await schedule_notify_flow(reminder_dt)
    write_lock_file(tz_name)


# ── Flow 2: Notifier ──────────────────────────────────────────────────────────

@flow(name=NOTIFY_DEPLOYMENT_NAME, on_failure=[notify_on_completion])
def sleep_reminder_notify_flow():
    """
    Fires at the time scheduled by sleep_reminder_schedule_flow.
    Sends an immediate Pushcut notification.
    No cron schedule — only ever created on-demand by the scheduler flow.
    """
    logger = get_run_logger()
    send_notification(
        title="🌙 Time to wind down",
        body="Put your watch on before you sleep.",
        time_sensitive=False,
    )
    logger.info("Sleep reminder notification sent")