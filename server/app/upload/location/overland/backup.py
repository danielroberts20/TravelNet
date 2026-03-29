import json
import logging
from datetime import datetime, timedelta, timezone

from config.general import LOCATION_OVERLAND_BACKUP_DIR
from models.telemetry import OverlandPayload

logger = logging.getLogger(__name__)


def append_to_daily_buffer(payload: OverlandPayload) -> None:
    """Append raw Overland payload to today's JSONL buffer file.

    Called as a background task on each Overland upload. Each line in the file
    is a JSON object representing one payload (i.e. one Overland batch). The
    file rolls over at UTC midnight naturally — a new date filename is created
    when the first upload of the day arrives.
    """
    today = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    path = LOCATION_OVERLAND_BACKUP_DIR / f"{today}.jsonl"
    with open(path, "a") as f:
        f.write(payload.model_dump_json() + "\n")


def log_previous_day_backup() -> None:
    """Log completion of the previous day's Overland buffer at INFO level.

    Called as a background task when a Shortcuts CSV uploads (~02:56 UTC),
    by which point yesterday's Overland file is complete. Counts payloads and
    total GPS points from the JSONL file and emits a single INFO log entry.
    Does nothing if no buffer file exists for yesterday (e.g. first run, or
    no Overland data was received that day).
    """
    yesterday = (datetime.now(tz=timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    path = LOCATION_OVERLAND_BACKUP_DIR / f"{yesterday}.jsonl"
    if not path.exists():
        return

    n_payloads = 0
    n_points = 0
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                n_payloads += 1
                n_points += len(data.get("locations", []))
            except (json.JSONDecodeError, AttributeError):
                pass

    if n_payloads == 0:
        return

    logger.info(
        "Overland daily backup: %s — %d payloads, %d points.",
        path.name, n_payloads, n_points,
    )
