from datetime import datetime, timezone
import logging
import sys
from logging.handlers import RotatingFileHandler
import threading
from email.message import EmailMessage
import smtplib

from config.general import ERROR_FILE, LOG_FILE, WARN_FILE
from database.util import get_conn
from notifications import send_email

class DailyDigestHandler(logging.Handler):
    
    def __init__(self):
        super().__init__(level=logging.WARNING)
        self._lock = threading.Lock()
        self._init_table()

    def _init_table(self):
        with get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS log_digest (
                    id        INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts        TEXT NOT NULL,
                    level     TEXT NOT NULL,
                    logger    TEXT NOT NULL,
                    module    TEXT NOT NULL,
                    lineno    INTEGER NOT NULL,
                    message   TEXT NOT NULL
                )
            """)

    def emit(self, record: logging.LogRecord):
        with self._lock:
            try:
                ts = datetime.fromtimestamp(record.created, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                with get_conn() as conn:
                    conn.execute(
                        "INSERT INTO log_digest (ts, level, logger, module, lineno, message) VALUES (?, ?, ?, ?, ?, ?)",
                        (ts, record.levelname, record.name, record.module, record.lineno, record.getMessage())
                    )
            except Exception:
                self.handleError(record)
    
    def flush_and_send(self, smtp_host, smtp_port, sender, password, recipient):
       with self._lock:
           with get_conn() as conn:
               rows = conn.execute(
                   "SELECT ts, level, logger, module, lineno, message FROM log_digest ORDER BY id"
               ).fetchall()
           if not rows:
               return
           with get_conn() as conn:
               conn.execute("DELETE FROM log_digest")

       try:
           now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
           lines = [
               f"TravelNet daily alert digest — {now}",
               f"{len(rows)} event(s) at WARNING or above:\n",
               "=" * 60,
           ]
           for ts, level, logger, module, lineno, message in rows:
               lines.append(f"{ts} | {level} | {logger} | {module}:{lineno} | {message}")
           lines.append("=" * 60)

           send_email(
               subject=f"[TravelNet] {len(rows)} alert(s) — {now}",
               body="\n".join(lines),
               smtp_host=smtp_host,
               smtp_port=smtp_port,
               sender=sender,
               password=password,
               recipient=recipient,
           )
       except Exception as e:
           with get_conn() as conn:                          # restore on failure
               conn.executemany(
                   "INSERT INTO log_digest (ts, level, logger, module, lineno, message) VALUES (?, ?, ?, ?, ?, ?)",
                   rows,
               )
           raise


digest_handler = DailyDigestHandler()

def configure_logging():
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    # All INFO+ logs
    info_handler = RotatingFileHandler(LOG_FILE, maxBytes=5*1024*1024, backupCount=5)
    info_handler.setLevel(logging.INFO)
    info_handler.setFormatter(formatter)

    # WARN+ logs
    warn_handler = RotatingFileHandler(WARN_FILE, maxBytes=5*1024*1024, backupCount=5)
    warn_handler.setLevel(logging.WARNING)
    warn_handler.setFormatter(formatter)

    # ERROR+ logs
    error_handler = RotatingFileHandler(ERROR_FILE, maxBytes=5*1024*1024, backupCount=5)
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)

    # Stream logs to stdout
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    # Daily email digest (WARNING+, only if events exist)
    digest_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(info_handler)
    root_logger.addHandler(warn_handler)
    root_logger.addHandler(error_handler)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(digest_handler)

    