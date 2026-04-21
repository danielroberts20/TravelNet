"""
ingest_revolut.py — Normalise and ingest a Revolut CSV export into TravelNet transactions table.

Usage:
    python ingest_revolut.py --db /path/to/travelnet.db --file account-statement.csv

Revolut exports all currencies in a single file. The source label is always 'revolut'.
"""

import argparse
import csv
import hashlib
import io
import json
from datetime import datetime
import logging
from database.location.geocoding import get_place_id
from notifications import send_notification
from database.exchange.fx import convert_to_gbp
from database.connection import get_conn, to_iso_str
from database.transaction.ingest.util import get_closest_lat_lon_by_timestamp, safe_float

logger = logging.getLogger(__name__)

# Revolut Type values that represent internal moves (between own Revolut accounts/vaults)
INTERNAL_TYPES = {"EXCHANGE"}
INTERNAL_DESCRIPTION_KEYWORDS = [
    "to savings",
    "from savings",
    "converted to",
    "converted from",
    "to vault",
    "from vault",
    "to pocket",
    "from pocket"
]

INTEREST_DESCRIPTION_KEYWORDS = ["interest", "cashback"]


def _is_internal(tx_type: str, description: str) -> bool:
    """Return True if the transaction represents an internal Revolut pot/vault move."""
    if tx_type.upper() in INTERNAL_TYPES:
        return True
    desc_lower = description.lower()
    return any(kw in desc_lower for kw in INTERNAL_DESCRIPTION_KEYWORDS)


def _is_interest(description: str) -> bool:
    """Return True if the transaction description indicates interest or cashback."""
    desc_lower = description.lower()
    return any(kw in desc_lower for kw in INTEREST_DESCRIPTION_KEYWORDS)


def _map_detail_type(tx_type: str) -> str:
    """Map Revolut's Type column to a normalised transaction_detail value."""
    mapping = {
        "CARD PAYMENT": "CARD_PAYMENT",
        "ATM": "ATM",
        "TRANSFER": "TRANSFER",
        "EXCHANGE": "EXCHANGE",
        "TOPUP": "TOPUP",
        "REFUND": "REFUND",
        "REWARD": "REWARD",
        "CASHBACK": "CASHBACK",
    }
    return mapping.get(tx_type.upper(), tx_type.upper())


def _generate_id(row: dict) -> str:
    """Revolut has no transaction ID — generate a stable hash from key fields."""
    key = f"{row.get('Started Date', '')}-{row.get('Amount', '')}-{row.get('Currency', '')}-{row.get('Description', '')}"
    return "REV-" + hashlib.sha256(key.encode()).hexdigest()[:16]


def _parse_timestamp(dt_str: str) -> str:
    """Parse Revolut datetime string to ISO8601. Format: '2026-02-28 11:56:12'"""
    dt = datetime.strptime(dt_str.strip(), "%Y-%m-%d %H:%M:%S")
    return dt.isoformat()


def insert(csv_text: str, source: str = "revolut"):
    """Parse and upsert all rows from a Revolut CSV export.

    Uses INSERT OR IGNORE so re-uploading the same file is safe.  Sends a
    Pushcut notification with a row count summary on completion.

    :returns: (inserted, skipped, errors) counts.
    """
    inserted = 0
    skipped = 0
    errors = 0

    reader = csv.DictReader(io.StringIO(csv_text))
    rows = list(reader)

    logger.info(f"Inserting {len(rows)} transactions from {source}...")

    with get_conn() as conn:
        cursor = conn.cursor()
        for row in rows:
            try:
                started_str = row.get("Started Date", "").strip()
                if not started_str:
                    logger.warning(f"Row missing Started Date. Skipping... {row}")
                    skipped += 1
                    continue

                # Use Started Date as canonical timestamp (when the user initiated the transaction)
                timestamp = to_iso_str(_parse_timestamp(started_str))
                tx_date = datetime.fromisoformat(timestamp).date()

                tx_id = _generate_id(row)

                amount = safe_float(row.get("Amount", ""))
                if amount is None:
                    logger.warning(f"Row missing amount. Skipping... {row}")
                    skipped += 1
                    continue

                currency = row.get("Currency", "").strip().upper()
                description = row.get("Description", "").strip()
                tx_type = row.get("Type", "").strip()
                state = row.get("State", "").strip().upper() or None
                fees = safe_float(row.get("Fee", "0")) or 0.0
                running_balance = safe_float(row.get("Balance", ""))

                detail_type = _map_detail_type(tx_type)
                transaction_type = "CREDIT" if amount >= 0 else "DEBIT"

                internal = 1 if _is_internal(tx_type, description) else 0
                interest = 1 if _is_interest(description) else 0

                amount_gbp = convert_to_gbp( amount, currency, tx_date)

                raw_json = json.dumps(dict(row), ensure_ascii=False)

                lat, lon = get_closest_lat_lon_by_timestamp(cursor, timestamp)
                logger.info(f"Closest lat/lon to transaction {tx_id} is {lat}, {lon}")
                place_id = get_place_id(lat, lon, conn=conn)
                logger.info(f"place_id: {place_id}")
                cursor.execute(
                    """
                    INSERT OR IGNORE INTO transactions (
                        id, source, bank, timestamp, amount, currency, amount_gbp,
                        description, payment_reference, payer, payee, merchant,
                        fees, transaction_type, transaction_detail, state,
                        is_internal, is_interest, running_balance, raw, place_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        tx_id, source, "Revolut", timestamp, amount, currency, amount_gbp,
                        description, None, None, None, None,
                        fees, transaction_type, detail_type, state,
                        internal, interest, running_balance, raw_json, place_id
                    ),
                )
                if cursor.rowcount == 1:
                    inserted += 1
                else:
                    logger.info(f"Duplicate transaction ID. Skipping... {tx_id} ({currency}) ({description[:40]})")
                    skipped += 1

            except Exception as e:
                logger.error(f"Error when processing transaction {row}")
                errors += 1

        conn.commit()
        
    send_notification(title="Revolut", 
                      body=f"{len(rows)} rows received | {inserted} inserted | {skipped} skipped",
                      time_sensitive=False)
    
    return inserted, skipped, errors
