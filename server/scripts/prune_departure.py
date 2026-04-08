"""
scripts/prune_departure.py

Delete all pre-departure data before a given cutoff datetime.
Takes a backup first, then deletes rows from the selected tables.

Usage:
    python -m scripts.prune_departure --before 2026-09-15T07:00:00
    python -m scripts.prune_departure --before 2026-09-15T07:00:00 --tables location_overland --tables transactions
    python -m scripts.prune_departure --before 2026-09-15T07:00:00 --dry-run
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import datetime

from app.config.general import DB_FILE
from app.database.pruning import DEFAULT_TABLES, TABLE_CONFIG, get_prune_counts, prune_before, validate_tables
from app.database.connection import backup_db


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Prune pre-departure data from TravelNet DB."
    )
    parser.add_argument(
        "--before",
        required=True,
        metavar="DATETIME",
        help="Delete rows with timestamps strictly before this value (ISO8601, e.g. 2026-09-15T07:00:00)",
    )
    parser.add_argument(
        "--tables",
        action="append",
        metavar="TABLE",
        default=None,
        help=(
            "Table to include (repeatable). Defaults to all time-series tables. "
            f"Available: {', '.join(TABLE_CONFIG.keys())}"
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print row counts that would be deleted and exit without making changes.",
    )
    return parser.parse_args()


def _format_counts(counts: dict[str, int]) -> str:
    lines = []
    total = 0
    for table, count in counts.items():
        if count == -1:
            lines.append(f"  {table:<25}  (cascade from parent)")
        else:
            lines.append(f"  {table:<25}  {count:>8,} rows")
            total += count
    lines.append(f"  {'TOTAL':<25}  {total:>8,} rows")
    return "\n".join(lines)


def main() -> None:
    args = parse_args()

    # Validate cutoff
    try:
        cutoff_dt = datetime.fromisoformat(args.before)
    except ValueError:
        print(f"ERROR: --before value '{args.before}' is not a valid ISO8601 datetime.", file=sys.stderr)
        sys.exit(1)

    # Resolve table list
    tables = args.tables if args.tables is not None else DEFAULT_TABLES

    try:
        validate_tables(tables)
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    cutoff_str = cutoff_dt.isoformat()
    print(f"\nTravelNet — Departure Prune")
    print(f"Cutoff : {cutoff_str}")
    print(f"Tables : {len(tables)} selected")
    print()

    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA foreign_keys = ON")

    # Dry-run counts
    counts = get_prune_counts(conn, cutoff_str, tables)
    print("Rows that will be deleted:")
    print(_format_counts(counts))
    print()

    if args.dry_run:
        print("Dry run complete. No changes made.")
        conn.close()
        return

    total = sum(c for c in counts.values() if c != -1)
    if total == 0:
        print("Nothing to delete. Exiting.")
        conn.close()
        return

    # Confirmation
    confirm = input('Type "DELETE" to proceed with backup + prune: ').strip()
    if confirm != "DELETE":
        print("Aborted.")
        conn.close()
        return

    print()

    # Backup first
    print("Creating pre-prune backup...")
    try:
        backup_path = backup_db(prefix="pre_prune")
        print(f"Backup created: {backup_path}")
    except Exception as e:
        print(f"ERROR: Backup failed — aborting prune. ({e})", file=sys.stderr)
        conn.close()
        sys.exit(1)

    # Prune
    print("Pruning...")
    deleted = prune_before(conn, cutoff_str, tables)
    conn.close()

    print()
    print("Deleted:")
    print(_format_counts(deleted))
    print()
    print("Done.")


if __name__ == "__main__":
    main()