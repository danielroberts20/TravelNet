import logging
import sys
from logging.handlers import RotatingFileHandler

from config.general import ERROR_FILE, LOG_FILE, WARN_FILE

def configure_logging():
    # All INFO+ logs
    info_handler = RotatingFileHandler(LOG_FILE, maxBytes=5*1024*1024, backupCount=5)
    info_handler.setLevel(logging.INFO)
    info_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s"))

    # WARN+ logs
    warn_handler = RotatingFileHandler(WARN_FILE, maxBytes=5*1024*1024, backupCount=5)
    warn_handler.setLevel(logging.WARNING)
    warn_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s"))

    # ERROR+ logs
    error_handler = RotatingFileHandler(ERROR_FILE, maxBytes=5*1024*1024, backupCount=5)
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s"))

    # Stream logs to stdout as well
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s"))

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(info_handler)
    root_logger.addHandler(warn_handler)
    root_logger.addHandler(error_handler)
    root_logger.addHandler(console_handler)