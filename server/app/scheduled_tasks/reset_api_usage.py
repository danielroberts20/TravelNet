"""
scheduled_tasks/reset_api_usage.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Reset the monthly API call counters for all external services (exchangerate.host
and open-meteo) so quota tracking starts fresh at the beginning of each month.

Scheduled to run on the 1st of each month.
"""
from config.editable import load_overrides
load_overrides()

import logging

from config.logging import configure_logging
from config.settings import settings
from database.exchange.util import reset_api_usage
from notifications import CronJobMailer
from config.editable import load_overrides


logger = logging.getLogger(__name__)


if __name__ == "__main__":
    configure_logging()
    logger.info("Resetting FX API usage counter...")
    services = ["exchangerate.host", "open-meteo"]

    with CronJobMailer("reset_api_usage", settings.smtp_config,
                       detail="Reset monthly API call counters") as job:
        for name in services:
            result = reset_api_usage(name)
            job.add_metric("service", result["service"])
            job.add_metric(f"{name} old count", result["old_count"])
            job.add_metric(f"{name} old month", result["old_month"])