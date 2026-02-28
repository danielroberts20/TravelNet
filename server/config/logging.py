import logging
import sys
import os
from logging.handlers import RotatingFileHandler

from config.general import LOG_FILE

def configure_logging():
    # Rotating file handler: keeps file <5MB, 5 backups
    file_handler = RotatingFileHandler(LOG_FILE, maxBytes=5*1024*1024, backupCount=5)
    file_formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    file_handler.setFormatter(file_formatter)

    # Stream handler to stdout (existing behavior)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(file_formatter)

    logging.basicConfig(
        level=logging.INFO,
        handlers=[console_handler, file_handler]
    )