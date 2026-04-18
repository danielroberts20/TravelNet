from prefect import flow, task, get_run_logger
from database.connection import get_conn
from notifications import error_notification
from datetime import datetime, timezone, timedelta
from config.runtime import get_app_uptime


@task
def get_server_uptime() -> float:
    """Return seconds since TravelNet server started, read from shared volume."""
    log = get_run_logger()
    try:
        uptime = get_app_uptime()
        log.info(f"TravelNet uptime: {int(uptime)}s")
        return uptime
    except Exception as e:
        log.warning(f"Could not read app start time: {e}")
        return 0.0

@task
def get_last_heartbeat():
    log = get_run_logger()
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

@task
def check_staleness(last: dict | None, threshold_minutes: int = 10):
    log = get_run_logger()
    if last is None:
        log.warning("No watchdog heartbeat ever received.")
        return False, "no heartbeat ever received"

    received_at = datetime.fromisoformat(last["received_at"].replace("Z", "+00:00"))
    age = datetime.now(timezone.utc) - received_at
    stale = age > timedelta(minutes=threshold_minutes)

    log.info(f"Last heartbeat: {last['received_at']} ({age.seconds}s ago)")
    return not stale, f"last seen {int(age.total_seconds())}s ago"

@flow(name="Check Watchdog")
def check_watchdog_flow():
    log = get_run_logger()

    uptime = get_server_uptime()
    if uptime < 600:  # less than 10 minutes — same as staleness threshold
        log.info(f"TravelNet only up for {int(uptime)}s — skipping watchdog staleness check.")
        return
    
    last = get_last_heartbeat()
    healthy, detail = check_staleness(last)

    if not healthy:
        log.warning(f"Watchdog appears to be down: {detail}")
        error_notification(f"⚠️ Watchdog is not responding — {detail}")
    else:
        log.info(f"Watchdog ok — {detail}")