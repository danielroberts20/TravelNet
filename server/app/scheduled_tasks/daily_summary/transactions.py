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
from notifications import notify_on_completion, record_flow_result
from scheduled_tasks.daily_summary.base import Domain, never_auto_close
from config.general import BACKFILL_MONTHS


# ---------------------------------------------------------------------------
# Compute function
# ---------------------------------------------------------------------------

def compute_transaction_data(conn, ctx: dict) -> dict:
    """Aggregate transactions + compute col-normalised spend."""
    txn = _transactions(conn, ctx)
    country_code, city = _location_for_date(conn, ctx["date"])
    txn["spend_normalised"] = _normalise_spend(
        conn, txn["spend_gbp"], country_code, city
    )
    return txn


def _transactions(conn, ctx: dict) -> dict:
    """Aggregate transactions for the UTC window, excluding internal/interest/failed."""
    row = conn.execute("""
        SELECT
            COALESCE(SUM(amount_gbp), 0) AS spend_gbp,
            COUNT(*)                     AS n
        FROM transactions
        WHERE timestamp >= ? AND timestamp < ?
          AND is_internal = 0
          AND is_interest = 0
          AND (state != 'FAILED' OR state IS NULL)
          AND amount_gbp IS NOT NULL
    """, (ctx["utc_start"], ctx["utc_end"])).fetchone()

    count     = row["n"] or 0
    spend_gbp = round(abs(row["spend_gbp"]), 2) if count else None

    dom_cur     = None
    spend_local = None
    if count:
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
        if cur_row:
            dom_cur     = cur_row["currency"]
            spend_local = round(cur_row["total_local"], 2)

    return {
        "spend_gbp":         spend_gbp,
        "spend_local":       spend_local,
        "spend_currency":    dom_cur,
        "transaction_count": count,
    }


def _location_for_date(conn, local_date: str) -> tuple:
    """Read country_code + city from daily_summary (populated by location domain)."""
    row = conn.execute("""
        SELECT country_code, city FROM daily_summary WHERE date = ?
    """, (local_date,)).fetchone()
    if not row:
        return None, None
    return row["country_code"], row["city"]


def _normalise_spend(conn, spend_gbp, country_code, city) -> float | None:
    """
    Divide spend_gbp by Numbeo cost-of-living index, scaled by 100.
    Result is "spend as % of NYC norm" — 50 means spending half of NYC-
    equivalent, 120 means 20% above NYC-equivalent.
    """
    if spend_gbp is None or not country_code:
        return None

    try:
        col = None
        if city:
            row = conn.execute("""
                SELECT col_index FROM cost_of_living
                WHERE country_code = ? AND city = ? LIMIT 1
            """, (country_code, city)).fetchone()
            col = row["col_index"] if row else None
        if col is None:
            row = conn.execute("""
                SELECT col_index FROM cost_of_living
                WHERE country_code = ? AND city IS NULL LIMIT 1
            """, (country_code,)).fetchone()
            col = row["col_index"] if row else None

        return round(spend_gbp / col * 100, 2) if col else None
    except Exception as e:
        logger = get_run_logger()
        logger.warning("Failed to normalise spend. " \
        "Most likely `cost_of_living` table is not yet implemented.")
        return None


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
    on_completion=[notify_on_completion],
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


@flow(
    name="Backfill Transactions in Summary",
    on_completion=[notify_on_completion],
    on_failure=[notify_on_completion],
)
def backfill_transactions_in_summary_flow():
    """
    Monthly: recompute transaction columns for every date in the last
    BACKFILL_MONTHS months, then mark those dates spend_complete = 1.
    Scheduled: 2nd of each month at 08:30.
    """
    logger  = get_run_logger()
    dates   = find_dates_in_window()
    logger.info(f"Backfilling transactions for {len(dates)} dates")

    updated = 0
    failed  = 0
    for d in dates:
        try:
            TRANSACTIONS_DOMAIN.upsert_for_date(d)
            # Mark complete once the monthly pass has run
            with get_conn() as conn:
                conn.execute(
                    "UPDATE daily_summary SET spend_complete = 1 WHERE date = ?",
                    (d,),
                )
            updated += 1
        except Exception as e:
            logger.error(f"Failed to backfill transactions for {d}: {e}")
            failed += 1

    result = {"dates_considered": len(dates),
              "updated": updated, "failed": failed}
    record_flow_result(result)
    return result