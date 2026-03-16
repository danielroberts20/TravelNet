import logging

from config.logging import configure_logging
from database.exchange.util import reset_api_usage
from config.general import (
    EMAIL_PASSWORD, EMAIL_RECIPIENT, EMAIL_SENDER,
    SMTP_HOST, SMTP_PORT,
)
from notifications import CronJobMailer


logger = logging.getLogger(__name__)


if __name__ == "__main__":
    configure_logging()

    smtp_cfg = {
        "host": SMTP_HOST,
        "port": SMTP_PORT,
        "sender": EMAIL_SENDER,
        "password": EMAIL_PASSWORD,
        "recipient": EMAIL_RECIPIENT,
    }
    logger.info("Resetting FX API usage counter...")

    with CronJobMailer("reset_fx_api_usage", smtp_cfg) as job:
        result = reset_api_usage("exchangerate.host")
        job.add_metric("service", result["service"])
        job.add_metric("old count", result["old_count"])
        job.add_metric("old month", result["old_month"])