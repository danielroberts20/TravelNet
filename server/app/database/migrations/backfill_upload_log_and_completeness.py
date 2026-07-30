"""
database/migrations/backfill_upload_log_and_completeness.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
One-off backfill for the new upload_log table and the completeness flags
it now drives.

Context: upload_log did not exist before this migration, so every
historical Revolut/Wise upload never recorded which calendar month it
covered. This script:

  1. Reconstructs upload_log retroactively — one row per (source, month)
     with >=1 transaction from that source's bank in the transactions
     table. row_count is the real count; inferred=1 and uploaded_at is
     left NULL (never a fabricated real timestamp — see below).
  2. Recomputes spend_complete for every daily_summary row using the new
     upload_log-coverage predicate (scheduled_tasks/daily_summary/
     transactions.py:_upload_coverage_predicate).
  3. Recomputes pi_complete for every daily_summary row using the new
     data-presence predicate (scheduled_tasks/daily_summary/pi.py:
     _pi_data_present), reading the watchdog/power columns already stored
     on each row rather than re-querying source tables.
  4. Reports before/after flip counts (0→1, 1→0, unchanged) for both flags.

AMBIGUOUS MONTHS ARE NEVER AUTO-INSERTED:
  A (source, month) with ZERO transactions in the considered date range is
  indistinguishable — from transaction data alone — between a genuine
  zero-spend month and an upload that simply never happened. These are
  printed for manual review; nothing is written to upload_log for them.
  Confirm and add manually if appropriate.

inferred=1 / uploaded_at=NULL is used for backfilled rows deliberately,
since we don't know when (or if) the source file was actually uploaded —
a fabricated "now" timestamp would misrepresent history, and inferred
lets callers distinguish "reconstructed from transaction data" from "we
actually saw this upload happen". Real uploads via /upload/revolut and
/upload/wise always write inferred=0 with a genuine uploaded_at (and
promote a previously-inferred row to inferred=0 if it's later uploaded
for real — see UploadLogTable.insert()).

Like migrate_known_places_schema.py, everything runs inside one
transaction regardless of --apply, and the transaction is rolled back
unless --apply is passed — so dry-run output reflects the exact state
that would result (including recomputed completeness flags), not just
the isolated upload_log step.

Run from inside the Docker container (or any environment with access to
travel.db):

    # Step 1 — see what would change, nothing is written:
    python -m database.migrations.backfill_upload_log_and_completeness [--db /path/to/travel.db]

    # Step 2 — after reviewing, write the changes for real:
    python -m database.migrations.backfill_upload_log_and_completeness --apply
"""

import argparse
import sqlite3
from calendar import monthrange
from datetime import date, timedelta

# Logical upload_log source label -> the `bank` value it corresponds to in
# transactions. NOT the same as transactions.source, which for Wise is a
# per-account/pot identifier (e.g. "137103728_USD"), not the literal "wise".
SOURCE_TO_BANK = {
    "revolut": "Revolut",
    "wise":    "Wise",
}


def _month_bounds(year: int, month: int) -> tuple[str, str]:
    start = date(year, month, 1)
    end = date(year, month, monthrange(year, month)[1])
    return start.isoformat(), end.isoformat()


def _months_in_range(start: date, end: date):
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        yield y, m
        m = m + 1 if m < 12 else 1
        y = y if m != 1 else y + 1


def reconstruct_upload_log(conn: sqlite3.Connection, start: date, end: date) -> dict:
    """Insert upload_log rows for (source, month) combos with >=1 transaction.

    Prints ambiguous zero-transaction months for manual review instead of
    inserting them. Returns a summary dict.
    """
    inserted = []
    ambiguous = []
    skipped_existing = []

    for source, bank in SOURCE_TO_BANK.items():
        for year, month in _months_in_range(start, end):
            period_start, period_end = _month_bounds(year, month)

            existing = conn.execute("""
                SELECT 1 FROM upload_log
                WHERE source = ? AND period_start = ? AND period_end = ?
            """, (source, period_start, period_end)).fetchone()
            if existing:
                skipped_existing.append((source, f"{year:04d}-{month:02d}"))
                continue

            next_period_start = (date.fromisoformat(period_end) + timedelta(days=1)).isoformat()
            row = conn.execute("""
                SELECT COUNT(*) AS n FROM transactions
                WHERE bank = ? AND timestamp >= ? AND timestamp < ?
            """, (bank, f"{period_start}T00:00:00Z", f"{next_period_start}T00:00:00Z")).fetchone()
            count = row["n"]

            if count == 0:
                ambiguous.append((source, f"{year:04d}-{month:02d}"))
                continue

            print(f"  upload_log += {source} {year:04d}-{month:02d} row_count={count} "
                  f"(inferred=1, uploaded_at=NULL)")
            conn.execute("""
                INSERT INTO upload_log (source, period_start, period_end, row_count, inferred, uploaded_at)
                VALUES (?, ?, ?, ?, 1, NULL)
            """, (source, period_start, period_end, count))
            inserted.append((source, f"{year:04d}-{month:02d}", count))

    if ambiguous:
        print("\n  AMBIGUOUS — zero transactions found, NOT inserted, needs manual review:")
        for source, ym in ambiguous:
            print(f"    {source} {ym}: zero transactions — genuine zero-spend month, or missed upload?")

    return {"inserted": inserted, "ambiguous": ambiguous, "skipped_existing": skipped_existing}


def recompute_spend_complete(conn: sqlite3.Connection) -> dict:
    """Recompute spend_complete for every daily_summary row using the
    upload_log-coverage predicate. Reflects any upload_log rows just
    inserted by reconstruct_upload_log() in this same transaction."""
    from scheduled_tasks.daily_summary.transactions import _upload_coverage_predicate

    rows = conn.execute("SELECT date, spend_complete FROM daily_summary").fetchall()
    flips = {"0_to_1": 0, "1_to_0": 0, "unchanged": 0}
    for row in rows:
        new_val = 1 if _upload_coverage_predicate(row["date"], {}, conn) else 0
        old_val = row["spend_complete"] or 0
        if new_val == old_val:
            flips["unchanged"] += 1
            continue
        flips["0_to_1" if new_val == 1 else "1_to_0"] += 1
        conn.execute(
            "UPDATE daily_summary SET spend_complete = ? WHERE date = ?",
            (new_val, row["date"]),
        )
    return flips


def recompute_pi_complete(conn: sqlite3.Connection) -> dict:
    """Recompute pi_complete for every daily_summary row using the new
    data-presence predicate. Reads the watchdog/power columns already
    stored on each row rather than re-querying source tables."""
    from scheduled_tasks.daily_summary.pi import _pi_data_present, _NO_DATA_FALLBACK_DAYS
    from scheduled_tasks.daily_summary.base import _age_days

    rows = conn.execute("""
        SELECT date, pi_complete, watchdog_heartbeats_received, avg_w_pi
        FROM daily_summary
    """).fetchall()
    flips = {"0_to_1": 0, "1_to_0": 0, "unchanged": 0}
    for row in rows:
        data = {
            "watchdog_heartbeats_received": row["watchdog_heartbeats_received"],
            "avg_w_pi": row["avg_w_pi"],
        }
        if _pi_data_present(data):
            new_val = 1
        else:
            new_val = 1 if _age_days(row["date"]) >= _NO_DATA_FALLBACK_DAYS else 0
        old_val = row["pi_complete"] or 0
        if new_val == old_val:
            flips["unchanged"] += 1
            continue
        flips["0_to_1" if new_val == 1 else "1_to_0"] += 1
        conn.execute(
            "UPDATE daily_summary SET pi_complete = ? WHERE date = ?",
            (new_val, row["date"]),
        )
    return flips


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill upload_log retroactively and recompute spend_complete/pi_complete."
    )
    parser.add_argument("--db", default="/app/data/travel.db", help="Path to travel.db")
    parser.add_argument(
        "--apply", action="store_true",
        help="Write changes. Without this flag, everything runs but is rolled back (dry-run).",
    )
    parser.add_argument("--start-date", default=None, help="YYYY-MM-DD, defaults to TRAVEL_START_DATE")
    parser.add_argument("--end-date", default=None, help="YYYY-MM-DD, defaults to today")
    args = parser.parse_args()

    if args.start_date:
        start = date.fromisoformat(args.start_date)
    else:
        from config.general import TRAVEL_START_DATE
        start = TRAVEL_START_DATE.date()
    end = date.fromisoformat(args.end_date) if args.end_date else date.today()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"=== upload_log + completeness backfill ({mode}) ===")
    print(f"Date range considered: {start.isoformat()} .. {end.isoformat()}\n")

    try:
        conn.execute("BEGIN")

        print("--- Step 1: reconstruct upload_log ---")
        upload_summary = reconstruct_upload_log(conn, start, end)
        print(f"  inserted: {len(upload_summary['inserted'])}  "
              f"ambiguous (skipped): {len(upload_summary['ambiguous'])}  "
              f"already present: {len(upload_summary['skipped_existing'])}")

        print("\n--- Step 2: recompute spend_complete ---")
        spend_flips = recompute_spend_complete(conn)
        print(f"  0->1: {spend_flips['0_to_1']}  1->0: {spend_flips['1_to_0']}  "
              f"unchanged: {spend_flips['unchanged']}")

        print("\n--- Step 3: recompute pi_complete ---")
        pi_flips = recompute_pi_complete(conn)
        print(f"  0->1: {pi_flips['0_to_1']}  1->0: {pi_flips['1_to_0']}  "
              f"unchanged: {pi_flips['unchanged']}")

        if args.apply:
            conn.execute("COMMIT")
            print("\nChanges committed.")
        else:
            conn.execute("ROLLBACK")
            print("\nDry run — no changes written. Re-run with --apply to write.")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
