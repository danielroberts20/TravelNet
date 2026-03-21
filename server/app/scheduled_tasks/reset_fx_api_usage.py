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

    with CronJobMailer("reset_fx_api_usage", settings.smtp_config) as job:
        result = reset_api_usage("exchangerate.host")
        job.add_metric("service", result["service"])
        job.add_metric("old count", result["old_count"])
        job.add_metric("old month", result["old_month"])