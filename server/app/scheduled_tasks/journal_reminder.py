"""
journal_reminder.py
───────────────────
Two-flow system for adaptive journal nudges (same shape as sleep_reminder.py).

Flow 1 (journal-reminder-schedule): runs daily at JOURNAL_SCHEDULE_HOUR local.
  Decides between two modes from how many consecutive days have gone uncovered:

    daily    fewer than JOURNAL_BACKLOG_AFTER_DAYS missed days.
             One quick-entry nudge today, at the quietest moment the model finds.
    backlog  JOURNAL_BACKLOG_AFTER_DAYS or more missed days.
             Daily nudges stop. At most one full-length catch-up nudge per
             JOURNAL_BACKLOG_INTERVAL_DAYS, at the best weekday/hour in the next week.

  and creates a Prefect-scheduled run of Flow 2 at the chosen moment.

Flow 2 (journal-reminder-notify): no cron; only ever created by Flow 1.
  Re-checks at fire time and stays quiet if you already wrote, if the mode has
  changed, or if a nudge already went out today. Otherwise sends a Pushcut:
    daily   → the journal notification   (point it at the Journal Quick Shortcut)
    backlog → the retro notification     (point it at the Journal Full Shortcut)

Rules (all enforced in plan_reminder / should_send, which are pure and unit-tested):
  • one nudge per local day, at most
  • no nudge on flight / timezone-change days (they roll into the backlog)
  • no nudge within a wake/sleep buffer of your typical wake and sleep times
  • a day only counts as journalled per database.journal.coverage

The Shortcut a notification opens is configured inside the Pushcut app, not here.
"""
import json
from datetime import datetime, timedelta, timezone
from typing import Optional

from prefect import flow, get_run_logger
from prefect.client.orchestration import get_client
from prefect.states import Scheduled

from config.editable import load_overrides
load_overrides()

import config.general as cfg
from database.connection import get_conn
from database.journal import timing
from database.journal.coverage import covered_today, entry_saved_since, missed_streak
from notifications import journal_notification, notify_on_completion, log_on_success, record_flow_result

NOTIFY_DEPLOYMENT_NAME = "Send Journal Reminder"

T_SCHEDULED = "journal_nudge_scheduled"
T_DAILY = "journal_nudge_daily"
T_BACKLOG = "journal_nudge_backlog"
_TS = "%Y-%m-%dT%H:%M:%SZ"


def _fmt(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime(_TS)


# ── trigger_log helpers ───────────────────────────────────────────────────────

def _log(conn, trigger: str, now_utc: datetime, payload: dict) -> None:
    conn.execute("INSERT INTO trigger_log (trigger, fired_at, payload) VALUES (?, ?, ?)",
                 (trigger, _fmt(now_utc), json.dumps(payload)))


def nudged_since(conn, since_utc: datetime) -> bool:
    return conn.execute(
        "SELECT 1 FROM trigger_log WHERE trigger IN (?, ?) AND fired_at >= ? LIMIT 1",
        (T_DAILY, T_BACKLOG, _fmt(since_utc)),
    ).fetchone() is not None


def last_backlog_nudge(conn) -> Optional[datetime]:
    row = conn.execute("SELECT MAX(fired_at) AS t FROM trigger_log WHERE trigger = ?", (T_BACKLOG,)).fetchone()
    return datetime.strptime(row["t"], _TS).replace(tzinfo=timezone.utc) if row and row["t"] else None


def pending_schedule(conn, mode: str, now_utc: datetime) -> bool:
    """Is a run of this mode already scheduled for a future moment?"""
    rows = conn.execute(
        "SELECT payload FROM trigger_log WHERE trigger = ? AND fired_at >= ? ORDER BY id DESC",
        (T_SCHEDULED, _fmt(now_utc - timedelta(days=cfg.JOURNAL_BACKLOG_INTERVAL_DAYS + 1))),
    ).fetchall()
    for r in rows:
        try:
            p = json.loads(r["payload"])
            if p["mode"] == mode and datetime.fromisoformat(p["scheduled_at"]) > now_utc:
                return True
        except Exception:
            continue
    return False


# ── Decisions ─────────────────────────────────────────────────────────────────

def current_mode(conn, today) -> tuple[str, int]:
    streak = missed_streak(conn, today, cfg.JOURNAL_RETRO_COUNT_DAYS)
    return ("backlog" if streak >= cfg.JOURNAL_BACKLOG_AFTER_DAYS else "daily"), streak


def plan_reminder(conn, now_utc: datetime) -> dict:
    """Decide what, if anything, to schedule. Returns {action, mode, streak, when?, reason}."""
    transitions = timing.load_transitions(conn)
    now_local = timing.to_local(transitions, now_utc)
    today = now_local.date()
    midnight_utc = now_local.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc)

    mode, streak = current_mode(conn, today)
    base = {"mode": mode, "streak": streak}

    if nudged_since(conn, midnight_utc):
        return {**base, "action": "none", "reason": "already nudged today"}
    if pending_schedule(conn, mode, now_utc):
        return {**base, "action": "none", "reason": f"a {mode} nudge is already scheduled"}

    if mode == "daily":
        if covered_today(conn, today, cfg.JOURNAL_RETRO_COUNT_DAYS):
            return {**base, "action": "none", "reason": "today already journalled"}
        if timing.is_transit_day(conn, _fmt(midnight_utc), _fmt(midnight_utc + timedelta(days=1))):
            return {**base, "action": "none", "reason": "flight or timezone-change day"}
    else:
        last = last_backlog_nudge(conn)
        earliest = today
        if last is not None:
            earliest = max(today, timing.to_local(transitions, last).date()
                           + timedelta(days=cfg.JOURNAL_BACKLOG_INTERVAL_DAYS - 1))

    since = now_utc - timedelta(weeks=cfg.JOURNAL_ACTIVITY_WEEKS)
    steps, alive = timing.load_activity(conn, since, now_utc, transitions)
    quiet = timing.quiet_profile(steps, alive, today)
    written = timing.load_writing_times(conn, since, transitions)
    writing = timing.writing_profile(written, today)
    bounds = timing.load_sleep_bounds(conn, now_utc, transitions)
    common = dict(wake_buffer_min=cfg.JOURNAL_WAKE_BUFFER_MINUTES, sleep_buffer_min=cfg.JOURNAL_SLEEP_BUFFER_MINUTES,
                  learn_min_entries=cfg.JOURNAL_LEARN_MIN_ENTRIES, fallback_hour=cfg.JOURNAL_FALLBACK_HOUR)

    if mode == "daily":
        slot = timing.pick_today(now_local, bounds, quiet, writing, len(written), **common)
    else:
        slot = timing.pick_weekly(now_local, earliest, bounds, quiet, writing, len(written),
                                  days_ahead=cfg.JOURNAL_BACKLOG_INTERVAL_DAYS, **common)
    if slot is None:
        return {**base, "action": "none", "reason": "no quiet slot left in the window"}
    return {**base, "action": "schedule", "when": slot,
            "reason": f"{len(quiet)} weekday-hours profiled, {len(written)} entries, "
                      f"wake {bounds.wake_min // 60:02d}:{bounds.wake_min % 60:02d}, "
                      f"sleep {(bounds.onset_min % 1440) // 60:02d}:{bounds.onset_min % 60:02d}"}


def should_send(conn, mode: str, planned_created_at: str, now_utc: datetime) -> tuple[bool, str]:
    """Re-check at fire time. Returns (send, reason)."""
    transitions = timing.load_transitions(conn)
    now_local = timing.to_local(transitions, now_utc)
    midnight_utc = now_local.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc)

    if nudged_since(conn, midnight_utc):
        return False, "already nudged today"
    if entry_saved_since(conn, planned_created_at):
        return False, "an entry was saved since this was scheduled"
    now_mode, _ = current_mode(conn, now_local.date())
    if now_mode != mode:
        return False, f"mode changed ({mode} → {now_mode})"
    if mode == "daily" and covered_today(conn, now_local.date(), cfg.JOURNAL_RETRO_COUNT_DAYS):
        return False, "today already journalled"
    return True, "ok"


# ── Flows ─────────────────────────────────────────────────────────────────────

async def _schedule_notify_run(when: datetime, parameters: dict) -> None:
    async with get_client() as client:
        deployments = await client.read_deployments()
        target = next((d for d in deployments if d.name == NOTIFY_DEPLOYMENT_NAME), None)
        if target is None:
            raise RuntimeError(f"Deployment '{NOTIFY_DEPLOYMENT_NAME}' not found. "
                               "Ensure it is registered in deployments.py.")
        await client.create_flow_run_from_deployment(
            deployment_id=target.id, parameters=parameters, state=Scheduled(scheduled_time=when))


@flow(name="Schedule Journal Reminder", on_failure=[notify_on_completion], on_completion=[log_on_success])
async def journal_reminder_schedule_flow() -> dict:
    logger = get_run_logger()
    now_utc = datetime.now(timezone.utc)
    with get_conn(read_only=True) as conn:
        plan = plan_reminder(conn, now_utc)

    result = {"mode": plan["mode"], "streak": plan["streak"], "action": plan["action"], "reason": plan["reason"]}
    if plan["action"] == "schedule":
        when = plan["when"]
        await _schedule_notify_run(when, {"mode": plan["mode"], "planned_at": _fmt(now_utc)})
        with get_conn() as conn:
            _log(conn, T_SCHEDULED, now_utc, {"mode": plan["mode"], "scheduled_at": when.astimezone(timezone.utc).isoformat()})
        result["scheduled_for"] = when.isoformat()
        logger.info("Journal %s nudge scheduled for %s (%s)", plan["mode"], when.isoformat(), plan["reason"])
    else:
        logger.info("No journal nudge scheduled (%s mode, %d missed days): %s",
                    plan["mode"], plan["streak"], plan["reason"])
    record_flow_result(result)
    return result


@flow(name=NOTIFY_DEPLOYMENT_NAME, on_failure=[notify_on_completion], on_completion=[log_on_success])
def journal_reminder_notify_flow(mode: str = "daily", planned_at: str = "1970-01-01T00:00:00Z") -> dict:
    """Fires at the time chosen by the schedule flow. No cron: only created on demand."""
    logger = get_run_logger()
    now_utc = datetime.now(timezone.utc)
    with get_conn(read_only=True) as conn:
        send, reason = should_send(conn, mode, planned_at, now_utc)
        streak = current_mode(conn, timing.to_local(timing.load_transitions(conn), now_utc).date())[1]

    result = {"mode": mode, "sent": send, "reason": reason, "streak": streak}
    if send:
        if mode == "backlog":
            journal_notification(title="📝 Weekly catch-up",
                                 body=f"{streak} days since your last entry. One full entry covering the gap?",
                                 retro=True)
        else:
            journal_notification(title="📝 Quick entry?", body="A quiet moment: capture a line about today.")
        with get_conn() as conn:
            _log(conn, T_BACKLOG if mode == "backlog" else T_DAILY, now_utc, {"streak": streak})
        logger.info("Journal %s nudge sent", mode)
    else:
        logger.info("Journal %s nudge skipped: %s", mode, reason)
    record_flow_result(result)
    return result
