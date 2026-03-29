from config.editable import load_overrides
load_overrides()

import logging
from upload.transaction.constants import WISE_SOURCE_MAP
from config.settings import settings
from config.logging import configure_logging
from database.util import get_conn
from config.notifications import CronJobMailer

logger = logging.getLogger(__name__)


def backfill_gbp():
    """Attempt to fill NULL amount_gbp values using stored FX rates.

    Iterates all transactions where amount_gbp is NULL, looks up the FX rate
    for the transaction date, and updates the row.  GBP transactions are set
    to 1:1.  Transactions without an available FX rate are logged as warnings
    and left NULL.
    """
    with get_conn() as conn:
        # Fetch all transactions with NULL amount_gbp
        null_rows = conn.execute("""
            SELECT id, currency, source, amount, timestamp
            FROM transactions
            WHERE amount_gbp IS NULL
        """).fetchall()

        if not null_rows:
            logger.info("No NULL amount_gbp transactions found.")
            return {
                "backfilled": 0,
                "still_null": 0
            }

        logger.info(f"Found {len(null_rows)} transaction(s) with NULL amount_gbp, attempting backfill...")

        backfilled = 0
        still_null = []

        for row in null_rows:
            tx_id, currency, source, amount, timestamp = (
                row["id"], row["currency"], row["source"],
                row["amount"], row["timestamp"]
            )

            # Extract date portion from ISO8601 timestamp
            date = timestamp[:10]

            if currency == "GBP":
                # No conversion needed — amount_gbp == amount
                amount_gbp = amount
            else:
                fx_row = conn.execute("""
                    SELECT rate FROM fx_rates
                    WHERE date = ?
                      AND source_currency = 'GBP'
                      AND target_currency = ?
                """, (date, currency)).fetchone()

                if fx_row is None:
                    still_null.append({
                        "id": tx_id,
                        "currency": currency,
                        "source": source,
                        "date": date,
                    })
                    continue

                # fx_rates stores GBP→currency, so invert to get currency→GBP
                amount_gbp = round(amount / fx_row["rate"], 6)

            conn.execute("""
                UPDATE transactions
                SET amount_gbp = ?
                WHERE id = ? AND currency = ? AND source = ?
            """, (amount_gbp, tx_id, currency, source))
            backfilled += 1

        logger.info(f"Backfilled {backfilled} transaction(s)")

        if still_null:
            logger.warning(
                f"{len(still_null)} transaction(s) still NULL after backfill "
                f"(no FX rate available for those dates):\n"
                + "\n".join(
                    f"  id={r['id']} source={WISE_SOURCE_MAP.get(r['source'], r['source'])} currency={r['currency']} date={r['date']}"
                    for r in still_null
                )
            )
    return {
        "backfilled": backfilled,
        "still_null": len(still_null)
    }


if __name__ == "__main__":
    configure_logging()

    with CronJobMailer("backfill_gbp", settings.smtp_config, 
                       detail="Backfill NULL amount_gbp from DB FX data") as job:
        result = backfill_gbp()
        job.add_metric("backfilled", result["backfilled"])
        job.add_metric("still null", result["still_null"])
        if result["still_null"] > 0:
            job.add_metric("status", "partial - expect daily digest email")