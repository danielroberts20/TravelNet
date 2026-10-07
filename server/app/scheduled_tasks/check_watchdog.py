from datetime import datetime, timezone, timedelta

from prefect import flow, get_run_logger

from config.runtime import get_app_uptime
from database.connection import get_conn
from notifications import error_notification, notify_on_completion

# Plain functions, not Prefect tasks: this flow runs every 5 minutes, and each
# task run costs three state rows plus log lines. The happy path logs at DEBUG
# so successful runs ship nothing to the Prefect API log; problems still log
# at WARNING and notify.


def get_server_uptime() -> float:
    """Return seconds since TravelNet server started, read from shared volume."""
    try:
        return get_app_uptime()
    except Exception as e:
        get_run_logger().warning(f"Could not read app start time: {e}")
        return 0.0


def get_last_heartbeat() -> dict | None:
    conn = get_conn(read_only=True)
    try:
        row = conn.execute("""
            SELECT received_at, consecutive_failures
            FROM watchdog_heartbeat
            ORDER BY received_at DESC
            LIMIT 1
        """).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def evaluate_staleness(
    last: dict | None,
    threshold_minutes: int = 10,
    now: datetime | None = None,
) -> tuple[bool, str]:
    """Return (healthy, detail) for the most recent heartbeat row."""
    if last is None:
        return False, "no heartbeat ever received"

    now = now or datetime.now(timezone.utc)
    received_at = datetime.fromisoformat(last["received_at"].replace("Z", "+00:00"))
    age = now - received_at
    stale = age > timedelta(minutes=threshold_minutes)
    return not stale, f"last seen {int(age.total_seconds())}s ago"


@flow(name="Check Watchdog", on_failure=[notify_on_completion])
def check_watchdog_flow():
    log = get_run_logger()

    uptime = get_server_uptime()
    if uptime < 600:  # less than 10 minutes — same as staleness threshold
        log.debug(f"TravelNet only up for {int(uptime)}s — skipping watchdog staleness check.")
        return

    healthy, detail = evaluate_staleness(get_last_heartbeat())

    if not healthy:
        log.warning(f"Watchdog appears to be down: {detail}")
        error_notification(f"⚠️ Watchdog is not responding — {detail}")
    else:
        log.debug(f"Watchdog ok — {detail}")
