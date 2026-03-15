import logging

from config.logging import configure_logging
from database.exchange.util import reset_api_usage


logger = logging.getLogger(__name__)


if __name__ == "__main__":
    configure_logging()
    logger.info("Resetting FX API usage counter...")
    reset_api_usage()