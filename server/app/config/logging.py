from datetime import datetime, timezone
import logging
import queue
import sys
from logging.handlers import RotatingFileHandler
import threading

from config.general import ERROR_FILE, LOG_FILE, WARN_FILE
from database.connection import get_conn, to_iso_str

# Custom level: above INFO, for key informational events worth surfacing
IMPORTANT_LEVEL = 25
logging.addLevelName(IMPORTANT_LEVEL, "IMPORTANT")

def _important(self, message, *args, **kwargs):
    """Log at the custom IMPORTANT level (25), above INFO but below WARNING."""
    if self.isEnabledFor(IMPORTANT_LEVEL):
        self._log(IMPORTANT_LEVEL, message, args, **kwargs)

logging.Logger.important = _important


class ColouredFormatter(logging.Formatter):
    COLOURS = {
        "DEBUG":     "\033[36m",   # cyan
        "INFO":      "\033[32m",   # green
        "IMPORTANT": "\033[96m",   # bright cyan
        "WARNING":   "\033[33m",   # yellow
        "ERROR":     "\033[31m",   # red
        "CRITICAL":  "\033[35m",   # magenta
    }
    RESET = "\033[0m"

    def format(self, record):
        """Wrap the level name in ANSI colour codes before delegating to the base formatter."""
        colour = self.COLOURS.get(record.levelname, "")
        # Copy the record to avoid mutating the original (shared across handlers)
        record = logging.makeLogRecord(record.__dict__)
        record.levelname = f"{colour}{record.levelname}{self.RESET}"
        return super().format(record)

class DailyDigestHandler(logging.Handler):
    """Logging handler that persists WARNING+ records to the DB for a daily digest email.

    Records are written via a background thread to avoid blocking the logging call-site.
    Call flush_and_send() (e.g. from a cron job) to drain the table and send the email.
    """

    def __init__(self):
        """Create the handler instance. Call configure_logging() to activate it."""
        super().__init__(level=logging.WARNING)
        self._lock = threading.Lock()
        self._queue: queue.Queue = queue.Queue()

    def _init_table(self):
        """Create the log_digest table if it does not already exist."""
        from database.logging.digest.table import table as log_digest_table
        log_digest_table.init()

    def _start_worker(self):
        """Spawn the daemon thread that drains the queue into the DB."""
        t = threading.Thread(target=self._worker, daemon=True)
        t.start()

    def _worker(self):
        """Continuously consume records from the queue and INSERT them into the DB."""
        from database.logging.digest.table import table as log_digest_table, LogDigestRecord
        while True:
            record = self._queue.get()
            if record is None:
                break
            try:
                ts, level, logger_name, module, lineno, message = record
                log_digest_table.insert(LogDigestRecord(
                    ts=ts, level=level, logger_name=logger_name,
                    module=module, lineno=lineno, message=message,
                ))
            except Exception:
                pass  # Never let the digest handler crash the server

    def emit(self, record: logging.LogRecord):
        """Enqueue a log record tuple for async DB insertion."""
        ts = to_iso_str(datetime.fromtimestamp(record.created, tz=timezone.utc))
        self._queue.put((ts, record.levelname, record.name, record.module, record.lineno, record.getMessage()))

    def flush_and_send(self, smtp_host, smtp_port, sender, password, recipient, username):
        from notifications import send_email
        from database.logging.digest.table import table as log_digest_table
        with self._lock:
            rows = log_digest_table.fetch_and_clear()
            if not rows:
                return

        try:
            now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
            lines = [
                f"TravelNet daily alert digest — {now}",
                f"{len(rows)} event(s) at WARNING or above:\n",
                "=" * 60,
            ]
            for r in rows:
                lines.append(f"{r.ts} | {r.level} | {r.logger_name} | {r.module}:{r.lineno} | {r.message}")
            lines.append("=" * 60)

            send_email(
                subject=f"[TravelNet] {len(rows)} alert(s) — {now}",
                body="\n".join(lines),
                smtp_host=smtp_host,
                smtp_port=smtp_port,
                sender=sender,
                password=password,
                recipient=recipient,
                username=username,
            )
        except Exception:
            log_digest_table.restore(rows)
            raise


digest_handler = DailyDigestHandler()

def configure_logging():
    """Configure root logger with file, console, and digest handlers.

    Sets up four handlers:
      - info_handler:    RotatingFile at INFO+
      - warn_handler:    RotatingFile at WARNING+
      - error_handler:   RotatingFile at ERROR+
      - console_handler: coloured stdout at INFO+
      - digest_handler:  DB-backed WARNING+ handler for daily email digest
    """
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
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

    # Stream logs to stdout (coloured) — INFO+ so the /logs page can filter them
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(ColouredFormatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    ))

    # Daily email digest (WARNING+, only if events exist)
    digest_handler._init_table()
    digest_handler._start_worker()
    digest_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(info_handler)
    root_logger.addHandler(warn_handler)
    root_logger.addHandler(error_handler)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(digest_handler)

    