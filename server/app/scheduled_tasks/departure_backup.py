"""
One-off DB backups scheduled for each country departure date.

At server startup, reads COUNTRY_DEPARTURE_DATES and creates an asyncio task
for each future date that sleeps until that moment and then calls backup_db.run().
Tasks are stored in _scheduled_tasks so they can be inspected or cancelled.

If COUNTRY_DEPARTURE_DATES is overridden and the server is restarted, the new
dates take effect automatically (load_overrides patches the value before
on_startup fires).
"""

import asyncio
import logging
from datetime import datetime

logger = logging.getLogger(__name__)
_scheduled_tasks: list[asyncio.Task] = []


async def _await_and_backup(country: str, departure: datetime) -> None:
    delay = (departure - datetime.now()).total_seconds()
    if delay <= 0:
        return
    logger.info(
        "Departure backup for %s scheduled in %.0f s (at %s)",
        country, delay, departure.strftime("%Y-%m-%d %H:%M"),
    )
    await asyncio.sleep(delay)
    logger.info("Running departure backup for %s", country)
    try:
        from scheduled_tasks.backup_db import run
        result = run(prefix=country)
        logger.info("Departure backup for %s complete: %s", country, result)
    except Exception:
        logger.exception("Departure backup for %s failed", country)


def schedule_departure_backups(departure_dates: dict) -> None:
    """Cancel existing departure backup tasks and schedule new ones from departure_dates."""
    global _scheduled_tasks
    cancelled = sum(1 for t in _scheduled_tasks if not t.done() and not t.cancel())
    _scheduled_tasks = []

    now = datetime.now()
    scheduled = 0
    for country, dt in departure_dates.items():
        if not isinstance(dt, datetime):
            logger.warning("Skipping %s: value is not a datetime (%r)", country, dt)
            continue
        if dt <= now:
            logger.debug("Departure date for %s (%s) is in the past, skipping", country, dt.date())
            continue
        task = asyncio.create_task(_await_and_backup(country, dt))
        _scheduled_tasks.append(task)
        scheduled += 1

    if cancelled:
        logger.info("Cancelled %d existing departure backup task(s)", cancelled)
    logger.info(
        "Scheduled %d departure backup(s): %s",
        scheduled,
        {c: str(d.date()) for c, d in departure_dates.items() if isinstance(d, datetime) and d > now},
    )
