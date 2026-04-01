import csv
import hashlib
import io
import json
import logging
from datetime import datetime
import re
from zipfile import ZipFile

from database.transaction.ingest.util import get_closest_lat_lon_by_timestamp
from notifications import send_notification
from upload.transaction.constants import WISE_SOURCE_MAP
from database.exchange.util import convert_to_gbp
from database.connection import get_conn, to_iso_str

# Transaction detail types that indicate internal pot-to-pot moves
INTERNAL_DETAIL_TYPES = {"CONVERSION", "MONEY_ADDED"}

# Keywords in description that suggest internal moves
INTERNAL_DESCRIPTION_KEYWORDS = [
    "moved ",       # "Moved 26.97 USD to 🐲 South East Asia"
    "move to ",
    "transfer to pot",
    "transfer from pot",
    "converted "
]

# Detail types / descriptions that indicate interest payments
INTEREST_DETAIL_TYPES = {"ACCRUAL_CHECKOUT", "INTEREST", "INVESTMENT_TRADE_ORDER"}
INTEREST_DESCRIPTION_KEYWORDS = ["interest", "service fee"]

DETAIL_TYPE_MAP = {
    "CARD":           "CARD_PAYMENT",
    "DEPOSIT":        "DEPOSIT",
    "CONVERSION":     "CONVERSION",
    "TRANSFER":       "TRANSFER",
    "ACCRUAL_CHARGE": "ACCRUAL_CHARGE",
    "MONEY_ADDED":    "DEPOSIT",
}

logger = logging.getLogger(__name__)

def _is_internal(detail_type: str, description: str) -> bool:
    """Return True if the transaction represents an internal Wise pot-to-pot move."""
    desc_lower = description.lower()
    if detail_type in INTERNAL_DETAIL_TYPES:
        # CONVERSION can also be a real currency exchange — check description
        if any(kw in desc_lower for kw in INTERNAL_DESCRIPTION_KEYWORDS):
            return True
    return False


def _is_interest(detail_type: str, description: str) -> bool:
    """Return True if the transaction represents interest, fees, or investment activity."""
    if detail_type in INTEREST_DETAIL_TYPES:
        return True
    desc_lower = description.lower()
    return any(kw in desc_lower for kw in INTEREST_DESCRIPTION_KEYWORDS)


def _generate_id(row: dict) -> str:
    """Generate a stable ID from the raw row when no TransferWise ID is present."""
    key = f"{row.get('Date Time', '')}-{row.get('Amount', '')}-{row.get('Currency', '')}-{row.get('Description', '')}"
    return "GENERATED-" + hashlib.sha256(key.encode()).hexdigest()[:16]


def _parse_timestamp(date_time_str: str) -> str:
    """Parse Wise datetime string to ISO8601."""
    # Format: "05-02-2026 08:54:15.466"
    dt = datetime.strptime(date_time_str.strip(), "%d-%m-%Y %H:%M:%S.%f")
    return dt.isoformat()


def parse_wise_csv(csv_text: str, source: str) -> list[dict]:
    """Parse a Wise CSV export into a list of normalised transaction dicts.

    Filters out interest/fee rows (ACCRUAL_CHECKOUT etc.) and maps Wise column
    names to the shared transactions schema.  FX conversion to GBP is performed
    inline at the transaction date.
    """
    reader = csv.DictReader(io.StringIO(csv_text))
    rows = []
    for row in reader:
        detail_type_raw = row.get("Transaction Details Type", "")

        if detail_type_raw in INTEREST_DETAIL_TYPES:
            continue

        description  = row.get("Description") or None
        detail_type  = DETAIL_TYPE_MAP.get(detail_type_raw, detail_type_raw)
        currency     = row.get("Currency", "").strip().upper()
        amount       = float(row.get("Amount") or 0)
        fees_raw     = row.get("Total fees")

        date_time_str = row.get("Date Time", "").strip()
        if not date_time_str:
            logger.warning(f"Row missing Datetime. Skipping... {row}")
            continue

        timestamp = _parse_timestamp(date_time_str)
        tx_date = datetime.fromisoformat(timestamp).date()

        rows.append({
            "id":                 row.get("TransferWise ID"),
            "source":             source,
            "bank":               "Wise",
            "timestamp":          timestamp,
            "amount":             amount,
            "currency":           currency,
            "amount_gbp":         convert_to_gbp(amount, currency, tx_date),
            "description":        description,
            "payment_reference":  row.get("Payment Reference") or None,
            "payer":              row.get("Payer Name") or None,
            "payee":              row.get("Payee Name") or None,
            "merchant":           row.get("Merchant") or None,
            "fees":               float(fees_raw) if fees_raw else 0.0,
            "transaction_type":   row.get("Transaction Type") or None,
            "transaction_detail": detail_type or None,
            "state":              None,
            "is_internal":        1 if _is_internal(detail_type_raw, description) else 0,
            "is_interest":        1 if detail_type_raw == "ACCRUAL_CHARGE" else 0,
            "running_balance":    float(row.get("Running Balance") or 0) if row.get("Running Balance") else None,
            "raw":                json.dumps(dict(row)),
        })
    return rows

def insert(zf: ZipFile, csv_filename: str, source: str = "unknown"):
    """Parse a single Wise CSV inside a ZipFile and upsert its transactions.

    :param zf: open ZipFile object from the uploaded .zip.
    :param csv_filename: path within the zip to the target CSV.
    :param source: account identifier key (e.g. '137103728_USD').
    :returns: (results, errors) — list of per-file result dicts and error dicts.
    """
    results = []
    errors = []
    try:
        with zf.open(csv_filename) as f:
            csv_text = f.read().decode("utf-8")

            rows = parse_wise_csv(csv_text, source)

            if not rows:
                results.append({"file": csv_filename, "inserted": 0, "skipped": "empty or all filtered"})
                logger.info(f"No transactions found for Wise-{source} ({WISE_SOURCE_MAP.get(source, "Unknown Source")})")
                return len(results), 0, 0, errors

            inserted = 0

            with get_conn() as conn:
                cursor = conn.cursor()

                for row in rows:
                    logger.info("Row")
                    row["timestamp"] = to_iso_str(row["timestamp"])
                    lat, lon = get_closest_lat_lon_by_timestamp(cursor, row["timestamp"])
                    logger.info(f"Closest lat/lon to transaction {row["id"]} is {lat}, {lon}")
                    result = cursor.execute("""
                        INSERT OR IGNORE INTO transactions (
                            id, source, bank, timestamp, amount, currency,
                            amount_gbp, description, payment_reference, payer,
                            payee, merchant, fees, transaction_type, transaction_detail,
                            state, is_internal, is_interest, running_balance, raw
                        ) VALUES (
                            :id, :source, :bank, :timestamp, :amount, :currency,
                            :amount_gbp, :description, :payment_reference, :payer,
                            :payee, :merchant, :fees, :transaction_type, :transaction_detail,
                            :state, :is_internal, :is_interest, :running_balance, :raw
                        )
                    """, row)
                    inserted += result.rowcount

                conn.commit()
            results.append({"file": csv_filename, "inserted": inserted, "parsed": len(rows)})
            logger.info(f"Inserting {inserted} transactions from Wise-{source} ({WISE_SOURCE_MAP[source]})...")
    except Exception as e:
        errors.append({"file": csv_filename, "error": str(e)})
    
    total_inserted = 0
    total_skipped = 0
    for result in results:
        inserted = result["inserted"]
        total_inserted += inserted
        total_skipped += result["parsed"] - inserted
    
    return len(results), total_inserted, total_skipped, errors
