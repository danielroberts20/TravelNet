"""
config/pruning.py
~~~~~~~~~~~~~~~~~
Unified upload backup pruning — retention policy for raw upload backup files.
Called daily from backup_db_flow via the prune_upload_backups task.

Retention classes:
  A — Replay      (health, workouts):           90 days  — raw HAE payloads for parser replay
  B — Redundant   (location overland, shortcuts): 14 days — DB stores raw_json per point; short safety net only
  C — Primary     (journal):                     not pruned — ZIPs are the source data, not copies
  D — Recovery    (transactions: Revolut, Wise): not pruned — low volume, genuine re-ingest value
  E — Trivial     (FX, mood):                   not pruned — sub-5MB projected over 3 years
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path


@dataclass
class PruneResult:
    domain: str
    files_deleted: int
    bytes_freed: int

    @property
    def bytes_freed_kb(self) -> float:
        return round(self.bytes_freed / 1024, 1)


def prune_directory(directory: Path, retention_days: int, domain: str = "") -> PruneResult:
    """
    Delete all files in `directory` (non-recursive) whose mtime is older
    than `retention_days`. Uses mtime — works with any filename format
    (.json, .jsonl, .csv, .zip etc.). Does not recurse into subdirectories.
    """
    if not directory.exists():
        return PruneResult(domain=domain, files_deleted=0, bytes_freed=0)

    cutoff = datetime.now() - timedelta(days=retention_days)
    deleted = 0
    freed = 0

    for f in directory.iterdir():
        if not f.is_file():
            continue
        try:
            mtime = datetime.fromtimestamp(f.stat().st_mtime)
            if mtime < cutoff:
                size = f.stat().st_size
                f.unlink()
                deleted += 1
                freed += size
        except Exception:
            pass  # skip files we cannot stat or delete

    return PruneResult(domain=domain, files_deleted=deleted, bytes_freed=freed)


# Retention constants
HEALTH_RETENTION_DAYS   = 90   # Class A — replay value
LOCATION_RETENTION_DAYS = 14   # Class B — redundant with DB
