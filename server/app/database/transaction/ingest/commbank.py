"""
ingest/commbank.py — Parse and ingest a CommBank "Transaction Summary" PDF.

CommBank only exports PDF statements. Each transaction starts with a line of the form

    11 Sep 2026 Fast Transfer From Daniel James Roberts $1.87 $1.87

(date, details, signed amount, running balance) and may be followed by detail lines
(`Card xx0488`, `Value Date: 13/09/2026`, `PayID ...`, free-text memo).

The statement has no currency (always AUD), no transaction ID and no time of day.
The source label is always 'commbank'.

Privacy: the statement header (name, address, BSB, account number) is never parsed
into a row or stored in `raw` — parsing only starts at the transaction table header.
"""

import hashlib
import io
import json
import logging
import re
from datetime import date, datetime
from decimal import Decimal

from database.connection import get_conn, to_iso_str
from database.cost_of_living.queries import get_col_entry, get_uk_col_index
from database.exchange.fx import convert_to_gbp
from database.location.geocoding import get_place_id
from database.transaction.ingest.util import get_nearest_lat_lon_within_hours, maybe_mark_internal
from notifications import send_notification # required for tests

logger = logging.getLogger(__name__)

CURRENCY = "AUD"

_MONEY = r"-?\$[\d,]+\.\d{2}"
_TX_LINE = re.compile(rf"^(\d{{2}} [A-Za-z]{{3}} \d{{4}}) (.+) ({_MONEY}) ({_MONEY})$")
_CARD_LINE = re.compile(r"^Card xx(\d{4})$")
_VALUE_DATE_LINE = re.compile(r"^Value Date: (\d{2}/\d{2}/\d{4})$")

_TABLE_HEADER = "Date Transaction details Amount Balance"
_TABLE_END = "Any pending transactions"
# Repeated on every page — never part of a transaction
_FURNITURE_PREFIXES = ("Account Number", "Page ", "Created ", "While this letter", "we’re not responsible")
_NOISE_LINES = {"CREDIT TO ACCOUNT"}

INTEREST_DESCRIPTION_KEYWORDS = ["interest"]


class CommbankParseError(ValueError):
    """The PDF is not a CommBank transaction summary, or its layout has changed."""


def _money_to_cents(s: str) -> int:
    """'-$1,234.56' -> -123456. Integer cents keep balance checks exact."""
    negative = s.startswith("-")
    return (-1 if negative else 1) * int(Decimal(s.lstrip("-").replace("$", "").replace(",", "")) * 100)


def _map_detail_type(description: str, has_card: bool, amount_cents: int) -> str:
    """Map a CommBank description to a normalised transaction_detail value."""
    desc = description.lower()
    if desc.startswith("wdl atm"):
        return "ATM"
    if "transfer" in desc:
        return "TRANSFER"
    if "interest" in desc:
        return "INTEREST"
    if has_card or amount_cents < 0:
        return "CARD_PAYMENT"
    return "DEPOSIT"


def _transfer_party(description: str) -> tuple[str | None, str | None]:
    """Return (payer, payee) for 'Fast Transfer From X' / 'Transfer To X' descriptions."""
    m = re.match(r"^(?:Fast )?Transfer (From|To) (.+)$", description, re.IGNORECASE)
    if not m:
        return None, None
    name = m.group(2).strip()
    return (name, None) if m.group(1).lower() == "from" else (None, name)


def parse_commbank_text(text: str) -> list[dict]:
    """Parse extracted statement text into raw transaction dicts (oldest first).

    Raises CommbankParseError if no transactions are found or the running
    balances do not reconcile — a mismatch means a row was mis-parsed or the
    PDF layout changed, and nothing should be ingested.
    """
    txs: list[dict] = []
    in_table = False

    for line in (l.strip() for l in text.splitlines()):
        if not line:
            continue
        if line == _TABLE_HEADER:
            in_table = True
            continue
        if not in_table:
            continue
        if line.startswith(_TABLE_END):
            break  # closing paragraph; nothing after this is a transaction
        if line.startswith(_FURNITURE_PREFIXES):
            continue

        m = _TX_LINE.match(line)
        if m:
            posted, details, amount, balance = m.groups()
            txs.append({
                "posted_date": datetime.strptime(posted, "%d %b %Y").date(),
                "description": details.strip(),
                "amount_cents": _money_to_cents(amount),
                "balance_cents": _money_to_cents(balance),
                "card_suffix": None,
                "value_date": None,
                "notes": [],
            })
            continue

        if not txs or line in _NOISE_LINES:
            continue
        cur = txs[-1]
        if (cm := _CARD_LINE.match(line)):
            cur["card_suffix"] = cm.group(1)
        elif (vm := _VALUE_DATE_LINE.match(line)):
            cur["value_date"] = datetime.strptime(vm.group(1), "%d/%m/%Y").date()
        else:
            cur["notes"].append(line)

    if not txs:
        raise CommbankParseError("No transactions found — is this a CommBank Transaction Summary PDF?")

    for prev, cur in zip(txs, txs[1:]):
        if prev["balance_cents"] + cur["amount_cents"] != cur["balance_cents"]:
            raise CommbankParseError(
                f"Running balance does not reconcile at {cur['posted_date']} "
                f"'{cur['description'][:40]}' — PDF layout may have changed"
            )
    return txs


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from every page of the PDF."""
    import pdfplumber  # lazy: only needed when a real PDF is parsed

    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            return "\n".join(page.extract_text() or "" for page in pdf.pages)
    except Exception as e:
        raise CommbankParseError(f"Could not read PDF: {e}") from e


def _assign_ids(txs: list[dict]) -> None:
    """Add a stable `id` to each tx.

    The statement has no transaction ID, and identical same-day charges are common,
    so the ID hashes (posted date, description, amount) plus the occurrence number
    of that combination within the statement. Unlike hashing the running balance,
    this is unaffected by other rows posting in between on overlapping uploads.
    """
    seen: dict[tuple, int] = {}
    for tx in txs:
        key = (tx["posted_date"].isoformat(), tx["description"], tx["amount_cents"])
        n = seen.get(key, 0)
        seen[key] = n + 1
        tx["id"] = "CBA-" + hashlib.sha256(f"{key}-{n}".encode()).hexdigest()[:16]


def parse_commbank_pdf(pdf_bytes: bytes) -> list[dict]:
    """PDF bytes -> list of parsed transaction dicts with ids. Raises CommbankParseError."""
    txs = parse_commbank_text(extract_pdf_text(pdf_bytes))
    _assign_ids(txs)
    return txs


def _to_row(tx: dict, source: str) -> dict:
    """Build a transactions-table row from a parsed tx."""
    amount_cents = tx["amount_cents"]
    amount = amount_cents / 100
    description = tx["description"]
    payer, payee = _transfer_party(description)

    # The value date is when the card was actually used; the posted date can lag by days.
    # No time of day exists, so use noon UTC to keep the calendar date stable.
    effective_date: date = tx["value_date"] or tx["posted_date"]
    timestamp = to_iso_str(datetime.combine(effective_date, datetime.min.time()).replace(hour=12))

    raw = {
        "posted_date": tx["posted_date"].isoformat(),
        "value_date": tx["value_date"].isoformat() if tx["value_date"] else None,
        "description": description,
        "amount": amount,
        "balance": tx["balance_cents"] / 100,
        "card_suffix": tx["card_suffix"],
        "notes": tx["notes"],
    }
    row = {
        "id": tx["id"],
        "source": source,
        "bank": "CommBank",
        "timestamp": timestamp,
        "amount": amount,
        "currency": CURRENCY,
        "amount_gbp": convert_to_gbp(amount, CURRENCY, effective_date),
        "description": description,
        "payment_reference": " | ".join(tx["notes"]) or None,
        "payer": payer,
        "payee": payee,
        "merchant": None,
        "fees": 0.0,
        "transaction_type": "CREDIT" if amount_cents >= 0 else "DEBIT",
        "transaction_detail": _map_detail_type(description, tx["card_suffix"] is not None, amount_cents),
        "state": "COMPLETED",  # CommBank excludes pending transactions from statements
        "is_internal": 0,
        "is_interest": 1 if any(kw in description.lower() for kw in INTEREST_DESCRIPTION_KEYWORDS) else 0,
        "running_balance": tx["balance_cents"] / 100,
        "raw": json.dumps(raw, ensure_ascii=False),
    }
    return maybe_mark_internal(row)


def insert(txs: list[dict], source: str = "commbank"):
    """Insert parsed CommBank transactions (from parse_commbank_pdf).

    Re-uploading the same statement is safe — duplicates are silently skipped.

    :returns: (inserted, skipped, errors) counts.
    """
    inserted = 0
    skipped = 0
    errors = 0

    logger.info(f"Inserting {len(txs)} transactions from {source}...")

    with get_conn() as conn:
        cursor = conn.cursor()
        for tx in txs:
            try:
                row = _to_row(tx, source)

                # Date-only timestamp: use the nearest fix that day rather than a 15-minute look-back
                lat, lon = get_nearest_lat_lon_within_hours(cursor, row["timestamp"])
                place_id = get_place_id(lat, lon, conn=conn)

                cost_of_living = get_col_entry(lat=lat, lon=lon, conn=conn)
                col_id = cost_of_living["id"] if cost_of_living else None
                amount_normalised = (
                    row["amount_gbp"] * (get_uk_col_index(conn) / cost_of_living["col_index"])
                    if cost_of_living and row["amount_gbp"] is not None else None
                )

                cursor.execute(
                    """
                    INSERT OR IGNORE INTO transactions (
                        id, source, bank, timestamp, amount, currency, amount_gbp,
                        description, payment_reference, payer, payee, merchant,
                        fees, transaction_type, transaction_detail, state,
                        is_internal, is_interest, running_balance, raw, place_id,
                        col_id, amount_normalised
                    ) VALUES (:id, :source, :bank, :timestamp,
                        :amount, :currency, :amount_gbp, :description,
                        :payment_reference, :payer, :payee, :merchant,
                        :fees, :transaction_type, :transaction_detail,
                        :state, :is_internal, :is_interest, :running_balance,
                        :raw, :place_id, :col_id, :amount_normalised)
                    """,
                    row | {"place_id": place_id, "col_id": col_id, "amount_normalised": amount_normalised},
                )
                if cursor.rowcount == 1:
                    inserted += 1
                else:
                    skipped += 1
                    logger.info(f"Duplicate transaction. Skipping... {row['id']} ({tx['description'][:40]})")
            except Exception as e:
                logger.error(f"Error when processing CommBank transaction {tx.get('id')}: {str(e)}")
                errors += 1

        conn.commit()

    send_notification(
        title="CommBank",
        body=f"💵 {len(txs)} rows received | {inserted} inserted | {skipped} skipped",
        time_sensitive=False,
    )

    return inserted, skipped, errors
