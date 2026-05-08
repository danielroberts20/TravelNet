"""
scheduled_tasks/daily_summary/transactions.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Owns the spending columns of daily_summary.

Unlike health/location/pi, this domain isn't closed by calendar age —
transactions are only complete when a month's CSVs have been uploaded
and processed. The monthly backfill flow sets spend_complete = 1
explicitly when it has finished its pass.
"""
from config.editable import load_overrides
load_overrides()

from datetime import datetime, timedelta, timezone as dt_timezone

from prefect import flow, task
from prefect.logging import get_run_logger

from database.connection import get_conn
from database.cost_of_living.queries import get_col_entry, get_uk_col_index
from notifications import notify_on_completion, record_flow_result
from scheduled_tasks.daily_summary.base import Domain, never_auto_close
from config.general import BACKFILL_MONTHS, TRANSACTION_COL_BATCH_SIZE


# ---------------------------------------------------------------------------
# Compute function
# ---------------------------------------------------------------------------

def compute_transaction_data(conn, ctx: dict) -> dict:
    """Aggregate transactions for the given UTC window."""
    return _transactions(conn, ctx)


def _transactions(conn, ctx: dict) -> dict:
    """
    Aggregate transactions for the UTC window, excluding internal/interest/failed.
 
    amount_normalised is populated by the monthly backfill flow — it will be
    NULL for any transactions that have not yet been through that flow, so
    spend_normalised may be NULL or partial until backfill has run.
    """
    row = conn.execute("""
        SELECT
            COALESCE(SUM(ABS(amount_gbp)), 0)         AS spend_gbp,
            COALESCE(SUM(ABS(amount_normalised)), 0)  AS spend_normalised,
            COUNT(*)                                  AS n,
            SUM(CASE WHEN amount_normalised IS NOT NULL
                     THEN 1 ELSE 0 END)               AS n_normalised
        FROM transactions
        WHERE timestamp >= ? AND timestamp < ?
          AND is_internal = 0
          AND is_interest = 0
          AND (state != 'FAILED' OR state IS NULL)
          AND amount_gbp IS NOT NULL
    """, (ctx["utc_start"], ctx["utc_end"])).fetchone()
 
    count        = row["n"] or 0
    n_normalised = row["n_normalised"] or 0
 
    if not count:
        return {
            "spend_gbp":         None,
            "spend_local":       None,
            "spend_currency":    None,
            "transaction_count": 0,
            "spend_normalised":  None,
        }
 
    spend_gbp        = round(row["spend_gbp"], 2)
    # Only store spend_normalised if at least one transaction was normalised.
    # Partial normalisation (some NULL) is accepted — Trevor/ML can check
    # n_normalised vs transaction_count if full coverage matters.
    spend_normalised = round(row["spend_normalised"], 2) if n_normalised else None
 
    # Dominant local currency (by transaction count)
    cur_row = conn.execute("""
        SELECT currency,
               COUNT(*)         AS n,
               SUM(ABS(amount)) AS total_local
        FROM transactions
        WHERE timestamp >= ? AND timestamp < ?
          AND is_internal = 0
          AND is_interest = 0
          AND (state != 'FAILED' OR state IS NULL)
        GROUP BY currency
        ORDER BY n DESC
        LIMIT 1
    """, (ctx["utc_start"], ctx["utc_end"])).fetchone()
 
    dom_cur     = cur_row["currency"] if cur_row else None
    spend_local = round(cur_row["total_local"], 2) if cur_row else None
 
    return {
        "spend_gbp":         spend_gbp,
        "spend_local":       spend_local,
        "spend_currency":    dom_cur,
        "transaction_count": count,
        "spend_normalised":  spend_normalised,
    }

# ---------------------------------------------------------------------------
# Domain spec
# ---------------------------------------------------------------------------

TRANSACTIONS_DOMAIN = Domain(
    name="transactions",
    columns=frozenset({
        "spend_gbp", "spend_local", "spend_currency",
        "transaction_count", "spend_normalised",
    }),
    completeness_flag="spend_complete",
    compute_fn=compute_transaction_data,
    # Not auto-closed — the monthly flow sets spend_complete = 1 explicitly
    # after it has processed a full month's data.
    completeness_predicate=never_auto_close,
)


# ---------------------------------------------------------------------------
# Flows
# ---------------------------------------------------------------------------

@flow(
    name="Compute Daily Summary — Transactions",
    on_failure=[notify_on_completion],
)
def compute_transactions_flow(local_date: str) -> dict:
    """
    Ad-hoc single-date upsert. Does NOT set spend_complete — that only
    happens in the monthly backfill once the full month is processed.
    """
    logger = get_run_logger()
    data = TRANSACTIONS_DOMAIN.upsert_for_date(local_date)
    logger.info(f"{local_date}: transactions upserted "
                f"(gbp={data.get('spend_gbp')}, n={data.get('transaction_count')})")
    result = {"local_date": local_date, **data}
    record_flow_result(result)
    return result


@task
def find_dates_in_window() -> list[str]:
    cutoff = (datetime.now(dt_timezone.utc).date()
              - timedelta(days=30 * BACKFILL_MONTHS))
    with get_conn(read_only=True) as conn:
        rows = conn.execute("""
            SELECT date FROM daily_summary
            WHERE date >= ? ORDER BY date ASC
        """, (cutoff.isoformat(),)).fetchall()
    return [r["date"] for r in rows]


@task
def backfill_col_normalisation(force: bool = False) -> dict:
    """
    Populate col_id and amount_normalised on transactions that are missing them.
 
    Runs before the daily summary aggregation so that spend_normalised in
    daily_summary reflects the normalised values.
 
    Args:
        force: If True, reprocess all transactions in the backfill window
               regardless of whether col_id is already set. Use when CoL
               data has been updated (e.g. new Numbeo year).
 
    Returns dict with counts: processed, updated, skipped, failed.
    """
    logger = get_run_logger()
 
    cutoff = (datetime.now(dt_timezone.utc).date()
              - timedelta(days=30 * BACKFILL_MONTHS)).isoformat()
 
    where_clause = (
        "WHERE timestamp >= ? AND amount_gbp IS NOT NULL"
        if force else
        "WHERE timestamp >= ? AND amount_gbp IS NOT NULL AND col_id IS NULL"
    )
 
    with get_conn(read_only=True) as conn:
        uk_col = get_uk_col_index(conn)
        if not uk_col:
            logger.warning("UK CoL index not found — skipping CoL normalisation")
            return {"processed": 0, "updated": 0, "skipped": 0, "failed": 0}
 
        rows = conn.execute(f"""
            SELECT t.id, t.currency, t.source,
                t.amount_gbp,
                p.lat_snap, p.lon_snap
            FROM transactions t
            LEFT JOIN places p ON p.id = t.place_id
            {where_clause}
            ORDER BY t.timestamp ASC
        """, (cutoff,)).fetchall()
 
    total     = len(rows)
    updated   = 0
    skipped   = 0
    failed    = 0
 
    logger.info(f"CoL normalisation: {total} transactions to process "
                f"({'force' if force else 'NULL only'})")
 
    for batch_start in range(0, total, TRANSACTION_COL_BATCH_SIZE):
        batch = rows[batch_start: batch_start + TRANSACTION_COL_BATCH_SIZE]
        updates = []

        with get_conn() as conn:
            for row in batch:
                try:
                    entry = get_col_entry(row["lat_snap"], row["lon_snap"], conn)
                    if not entry or not entry["index_value"]:
                        logger.warning(
                            f"No CoL entry for transaction {row['id']} "
                            f"({row['lat_snap']}, {row['lon_snap']}) — skipping"
                        )
                        skipped += 1
                        continue
    
                    amount_normalised = round(
                        abs(row["amount_gbp"]) * (uk_col / entry["index_value"]), 2
                    )
                    updates.append((
                        entry["id"],
                        amount_normalised,
                        row["id"],
                        row["currency"],
                        row["source"],
                    ))
                except Exception as e:
                    logger.warning(
                        f"Failed to normalise transaction {row['id']}/{row['currency']}: {e}"
                    )
                    failed += 1
    
            if updates:
                conn.executemany("""
                    UPDATE transactions
                    SET col_id = ?, amount_normalised = ?
                    WHERE id = ? AND currency = ? AND source = ?
                """, updates)
                updated += len(updates)
    
            logger.debug(f"CoL batch {batch_start}–{batch_start + len(batch)}: "
                        f"{len(updates)} updated")
 
    logger.info(f"CoL normalisation complete: "
                f"{updated} updated, {skipped} skipped, {failed} failed")
    return {"processed": total, "updated": updated,
            "skipped": skipped, "failed": failed}

@flow(
    name="Backfill Transactions in Summary",
    on_failure=[notify_on_completion],
)
def backfill_transactions_in_summary_flow(force_col: bool = False):
    """
    Monthly: populate CoL normalisation on transactions, then recompute
    transaction columns for every date in the last BACKFILL_MONTHS months,
    then mark those dates spend_complete = 1.
    Scheduled: 2nd of each month at 08:30.
 
    Args:
        force_col: passed through to backfill_col_normalisation. Set True
                   when CoL data has been updated (e.g. new Numbeo year).
    """
    logger = get_run_logger()
 
    # Step 1: populate col_id + amount_normalised on transactions
    col_result = backfill_col_normalisation(force=force_col)
    logger.info(f"CoL normalisation: {col_result}")
 
    # Step 2: aggregate into daily_summary
    dates = find_dates_in_window()
    logger.info(f"Backfilling transactions for {len(dates)} dates")
 
    updated = 0
    failed  = 0
    for d in dates:
        try:
            TRANSACTIONS_DOMAIN.upsert_for_date(d)
            with get_conn() as conn:
                conn.execute(
                    "UPDATE daily_summary SET spend_complete = 1 WHERE date = ?",
                    (d,),
                )
            updated += 1
        except Exception as e:
            logger.error(f"Failed to backfill transactions for {d}: {e}")
            failed += 1
 
    result = {
        "col_normalisation":  col_result,
        "dates_considered":   len(dates),
        "updated":            updated,
        "failed":             failed,
    }
    record_flow_result(result)
    return result
 