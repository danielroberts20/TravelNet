"""
database/health/dedupe.py
~~~~~~~~~~~~~~~~~~~~~~~~~
Make the health tables' natural keys NULL-safe.

health_quantity declares UNIQUE(timestamp, metric, source), but SQLite treats
NULLs as distinct inside a UNIQUE constraint, so rows with ``source IS NULL``
(every summarised metric, and anything HAE sends without a source) were never
deduplicated: ``INSERT OR IGNORE`` inserted them again on each re-upload.

``ensure_null_safe_unique`` fixes that in two steps, once per table:
  1. delete duplicate rows, keeping the earliest (MIN(id)) - the same "first write
     wins" behaviour sourced rows already had;
  2. create a UNIQUE index on the key with the nullable column wrapped in
     COALESCE(col, ''), after which INSERT OR IGNORE dedupes NULL sources too.

It is a no-op once the index exists, so it is safe to call on every startup.
Stdlib only, so it can also be run directly against a DB file:

    python3 dedupe.py /path/to/travel.db [--dry-run]
"""

from __future__ import annotations

import sqlite3
import sys
from dataclasses import dataclass


@dataclass(frozen=True)
class NullSafeKey:
    table: str
    columns: tuple[str, ...]      # natural key, in order
    nullable: str                 # the column that may be NULL
    index: str

    @property
    def key_sql(self) -> str:
        return ", ".join(f"COALESCE({c}, '')" if c == self.nullable else c for c in self.columns)


HEALTH_QUANTITY = NullSafeKey("health_quantity", ("timestamp", "metric", "source"), "source", "uq_hquantity_key")
HEALTH_HEART_RATE = NullSafeKey("health_heart_rate", ("timestamp", "source"), "source", "uq_hheartrate_key")
HEALTH_SLEEP = NullSafeKey("health_sleep", ("start_ts", "stage", "source"), "source", "uq_hsleep_key")


def _index_exists(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='index' AND name=?", (name,)).fetchone() is not None


def duplicate_count(conn: sqlite3.Connection, key: NullSafeKey) -> int:
    """Rows that would be deleted."""
    return conn.execute(
        f"SELECT COALESCE(SUM(n - 1), 0) FROM "
        f"(SELECT COUNT(*) n FROM {key.table} GROUP BY {key.key_sql} HAVING n > 1)"
    ).fetchone()[0]


def ensure_null_safe_unique(conn: sqlite3.Connection, key: NullSafeKey) -> int:
    """Dedupe and index ``key``. Returns the number of rows deleted (0 if already done)."""
    if _index_exists(conn, key.index):
        return 0
    deleted = conn.execute(
        f"DELETE FROM {key.table} WHERE id IN ("
        f"  SELECT id FROM (SELECT id, ROW_NUMBER() OVER (PARTITION BY {key.key_sql} ORDER BY id) rn "
        f"                  FROM {key.table}) WHERE rn > 1)"
    ).rowcount
    conn.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS {key.index} ON {key.table}({key.key_sql})")
    return max(deleted, 0)


def _main(argv: list[str]) -> int:
    args = [a for a in argv if not a.startswith("--")]
    dry = "--dry-run" in argv
    if len(args) != 1:
        print(__doc__)
        return 2
    conn = sqlite3.connect(args[0], timeout=30)
    try:
        for key in (HEALTH_QUANTITY, HEALTH_HEART_RATE, HEALTH_SLEEP):
            if _index_exists(conn, key.index):
                print(f"{key.table}: already done")
            elif dry:
                print(f"{key.table}: would delete {duplicate_count(conn, key)} duplicate rows")
            else:
                n = ensure_null_safe_unique(conn, key)
                conn.commit()
                print(f"{key.table}: deleted {n} duplicate rows, created {key.index}")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(_main(sys.argv[1:]))
