import csv
import hashlib
import json
import logging
from datetime import datetime
from typing import Optional

from database.exchange.util import convert_to_gbp
from database.util import get_conn

# Transaction detail types that indicate internal pot-to-pot moves
INTERNAL_DETAIL_TYPES = {"CONVERSION"}

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

logger = logging.getLogger(__name__)

def _is_internal(detail_type: str, description: str) -> bool:
    desc_lower = description.lower()
    if detail_type in INTERNAL_DETAIL_TYPES:
        # CONVERSION can also be a real currency exchange — check description
        if any(kw in desc_lower for kw in INTERNAL_DESCRIPTION_KEYWORDS):
            return True
    return False


def _is_interest(detail_type: str, description: str) -> bool:
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


def _safe_float(value: str) -> Optional[float]:
    try:
        return float(value) if value.strip() != "" else None
    except (ValueError, AttributeError):
        return None


def insert(csv_path: str, source: str = "unknown"):
    inserted = 0
    skipped = 0
    errors = 0

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    logger.info(f"Inserting {len(rows)} transactions from Wise-{source}...")

    with get_conn() as conn:
        cursor = conn.cursor()

        for row in rows:
            try:
                raw_id = row.get("TransferWise ID", "").strip()
                tx_id = raw_id if raw_id else _generate_id(row)

                date_time_str = row.get("Date Time", "").strip()
                if not date_time_str:
                    logger.warning(f"Row missing Datetime. Skipping... {row}")
                    skipped += 1
                    continue

                timestamp = _parse_timestamp(date_time_str)
                tx_date = datetime.fromisoformat(timestamp).date()

                amount = _safe_float(row.get("Amount", ""))
                if amount is None:
                    logger.warning(f"Row missing amount. Skipping... {row}")
                    skipped += 1
                    continue

                currency = row.get("Currency", "").strip().upper()
                description = row.get("Description", "").strip()
                payment_reference = row.get("Payment Reference", "").strip() or None
                payer = row.get("Payer Name", "").strip() or None
                payee = row.get("Payee Name", "").strip() or None
                merchant = row.get("Merchant", "").strip() or None
                fees = _safe_float(row.get("Total fees", "0")) or 0.0
                running_balance = _safe_float(row.get("Running Balance", ""))
                transaction_type = row.get("Transaction Type", "").strip().upper() or None
                detail_type = row.get("Transaction Details Type", "").strip().upper() or None

                if detail_type == "INVESTMENT_TRADE_ORDER":
                    skipped += 1
                    continue

                internal = 1 if _is_internal(detail_type or "", description) else 0
                interest = 1 if _is_interest(detail_type or "", description) else 0

                amount_gbp = convert_to_gbp(amount, currency, tx_date)

                raw_json = json.dumps(dict(row), ensure_ascii=False)
                
                cursor.execute(
                    """
                    INSERT OR IGNORE INTO transactions (
                        id, source, bank, timestamp, amount, currency, amount_gbp,
                        description, payment_reference, payer, payee, merchant,
                        fees, transaction_type, transaction_detail, state,
                        is_internal, is_interest, running_balance, raw
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        tx_id, source, "Wise", timestamp, amount, currency, amount_gbp,
                        description, payment_reference, payer, payee, merchant,
                        fees, transaction_type, detail_type, None,
                        internal, interest, running_balance, raw_json,
                    ),
                )
                if cursor.rowcount == 1:
                    inserted += 1
                else:
                    logger.warning(f"Duplicate transaction ID. Skipping... {tx_id} ({currency}) ({description[:40]})")
                    skipped += 1

            except Exception as e:
                logger.error(f"Error when processing transaction {row}")
                errors += 1

        conn.commit()
        
    return inserted, skipped, errors
