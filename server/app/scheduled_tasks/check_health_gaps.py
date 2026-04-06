"""
scheduled_tasks/check_health_gaps.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Scan the health_quantity table for days that are missing all metrics, or that
have an incomplete set of expected metrics, since data collection began.

Scheduled: daily.  Logs a WARNING for each problematic day and returns a
summary dict with gap and partial counts so CronJobMailer can report them.
"""
from config.editable import load_overrides
load_overrides()

import logging
from datetime import date, timedelta
from config.editable import load_overrides
from config.logging import configure_logging
from database.connection import get_conn
from notifications import CronJobMailer
from config.settings import settings

logger = logging.getLogger(__name__)

EXPECTED_METRICS = {
    "Active Energy",
    "Apple Exercise Time",
    "Apple Stand Hour",
    "Apple Stand Time",
    "Blood Oxygen Saturation",
    "Environmental Audio Exposure",
    "Flights Climbed",
    "Heart Rate Variability",
    "Physical Effort",
    "Resting Energy",
    "Resting Heart Rate",
    "Heart Rate",
    "Step Count",
    "Time in Daylight",
    "Walking + Running Distance",
    "Walking Asymmetry Percentage",
    "Walking Double Support Percentage",
    "Walking Heart Rate Average",
    "Walking Speed",
    "Walking Step Length",
}


def check_health_gaps() -> dict:
    """Check for missing or incomplete health data days since collection began.

    Returns a dict with 'gaps' (days with zero data) and 'partial' (days
    missing at least one of the EXPECTED_METRICS).
    """
    with get_conn() as conn:
        # Earliest date in the table — use as floor so we don't warn before data collection began
        row = conn.execute(
            "SELECT MIN(DATE(timestamp)) FROM health_quantity"
        ).fetchone()
        if not row or not row[0]:
            logger.warning("Health gap check: no data in health_quantity table.")
            return {"gaps": 0, "partial": 0}

        start_date = date.fromisoformat(row[0])
        yesterday = date.today() - timedelta(days=1)

        # Fetch all (date, metric) pairs in range
        rows = conn.execute(
            """
            SELECT DATE(timestamp) AS day, metric
            FROM health_quantity
            WHERE DATE(timestamp) BETWEEN ? AND ?
            """,
            (start_date.isoformat(), yesterday.isoformat()),
        ).fetchall()

    # Build a dict of {date_str: set of metrics present}
    metrics_by_day: dict[str, set] = {}
    for day, metric in rows:
        metrics_by_day.setdefault(day, set()).add(metric)

    # Walk every day in range and check coverage
    total_days = (yesterday - start_date).days + 1
    gap_count = 0
    partial_count = 0

    current = start_date
    while current <= yesterday:
        day_str = current.isoformat()
        present = metrics_by_day.get(day_str, set())

        if not present:
            logger.warning("Health gap check: no data uploaded for %s.", day_str)
            gap_count += 1
        else:
            missing = EXPECTED_METRICS - present
            if missing:
                missing_sorted = ", ".join(sorted(missing))
                logger.warning(
                    "Health gap check: partial upload for %s — missing %d metric(s): %s.",
                    day_str,
                    len(missing),
                    missing_sorted,
                )
                partial_count += 1

        current += timedelta(days=1)

    logger.info(
        "Health gap check complete: %d day(s) checked, %d missing, %d partial.",
        total_days,
        gap_count,
        partial_count,
    )
    return {"gaps": gap_count, "partial": partial_count}


if __name__ == "__main__":
    configure_logging()

    with CronJobMailer("check_health_gaps", settings.smtp_config,
                       detail="Look for missing expected metrics in last week of health data") as job:
        metrics = check_health_gaps()
        job.add_metric("gaps", metrics["gaps"])
        job.add_metric("partial", metrics["partial"])