from config.editable import load_overrides
load_overrides()

import logging

from prefect import task, flow

from config.settings import settings
from database.connection import get_conn
from upload.transaction.constants import WISE_SOURCE_MAP
from notifications import notify_on_completion, record_flow_result


logger = logging.getLogger(__name__)


@task
def backfill_null_gbp() -> dict:
    with get_conn() as conn:
        null_rows = conn.execute("""
            SELECT id, currency, source, amount, timestamp
            FROM transactions
            WHERE amount_gbp IS NULL
        """).fetchall()

        if not null_rows:
            logger.info("No NULL amount_gbp transactions found.")
            return {"backfilled": 0, "still_null": 0}

        logger.info(f"Found {len(null_rows)} transaction(s) with NULL amount_gbp, attempting backfill...")

        backfilled = 0
        still_null = []

        for row in null_rows:
            tx_id, currency, source, amount, timestamp = (
                row["id"], row["currency"], row["source"],
                row["amount"], row["timestamp"]
            )
            date = timestamp[:10]

            if currency == "GBP":
                fx_rate = 1.0
            else:
                fx_row = conn.execute("""
                    SELECT rate FROM fx_rates
                    WHERE date = ?
                      AND source_currency = 'GBP'
                      AND target_currency = ?
                """, (date, currency)).fetchone()

                if not fx_row:
                    logger.warning(f"No FX rate found for {currency} on {date}")
                    still_null.append({"id": tx_id, "currency": currency, "source": source, "date": date})
                    continue

                fx_rate = fx_row["rate"]

            amount_gbp = round(amount / fx_rate, 6)

            cursor = conn.execute("""
                UPDATE transactions
                SET amount_gbp = ?
                WHERE id = ? AND currency = ? AND source = ?
            """, (amount_gbp, tx_id, currency, source))

            if cursor.rowcount == 1:
                backfilled += 1
            else:
                logger.error(f"Failed to update transaction id={tx_id} (rows affected: {cursor.rowcount})")
                still_null.append({"id": tx_id, "currency": currency, "source": source, "date": date})

        if still_null:
            logger.warning(
                f"{len(still_null)} transaction(s) still NULL after backfill:\n"
                + "\n".join(
                    f"  id={r['id']} source={WISE_SOURCE_MAP.get(r['source'], r['source'])} "
                    f"currency={r['currency']} date={r['date']}"
                    for r in still_null
                )
            )

        logger.info(f"Backfilled {backfilled} transaction(s)")
        return {"backfilled": backfilled, "still_null": len(still_null)}


@flow(name="Backfill GBP", on_failure=[notify_on_completion])
def backfill_gbp_flow():
    result = backfill_null_gbp()
    record_flow_result(result)
    return result