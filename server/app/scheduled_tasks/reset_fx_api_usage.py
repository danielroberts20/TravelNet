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

    with CronJobMailer("reset_fx_api_usage", settings.smtp_config,
                       detail="Reset monthly API call counters") as job:
        for service in services:
            result = reset_api_usage(services)
            job.add_metric("service", result["service"])
            job.add_metric(f"{service} old count", result["old_count"])
            job.add_metric(f"{service} old month", result["old_month"])