"""
health_gaps/ingest.py
~~~~~~~~~~~~~~~~~~~~~
Feed pulled data through the normal health ingest, exactly as a REST upload would be.

The raw payload is appended to today's JSONL backup first (same layout as the
/upload/health/data route writes), so a recovery can be replayed from disk.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

logger = logging.getLogger(__name__)


def backup_payload(data: dict, directory: Path, when: Optional[datetime] = None) -> Path:
    """Append ``{"data": data}`` as one line to ``<directory>/<YYYY-MM-DD>.jsonl``."""
    when = when or datetime.now()
    path = directory / f"{when.strftime('%Y-%m-%d')}.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps({"data": data}, ensure_ascii=False) + "\n")
    return path


def make_ingest(directory: Path, handler: Optional[Callable[[dict], None]] = None) -> Callable[[dict], None]:
    """Return ``ingest(data)``: back the payload up, then hand it to the health upload handler.

    The handler defaults to upload.health.processing.handle_health_upload, imported lazily so
    this module stays light. It logs and skips a bad metric rather than raising; the rescan
    after the pull is what verifies that data actually landed.
    """
    def ingest(data: dict) -> None:
        backup_payload(data, directory)
        h = handler
        if h is None:
            from upload.health.processing import handle_health_upload as h  # noqa: N813
        h(data)
    return ingest
