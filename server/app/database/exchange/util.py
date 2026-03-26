from datetime import date, datetime, timedelta
import json
import logging
from typing import Optional

from database.exchange.table import insert_fx_rate
from config.general import SOURCE_CURRENCY
from database.util import get_conn


logger = logging.getLogger(__name__)

def get_gbp_rate(currency: str, on_date: date, tolerance_days: int = 7) -> Optional[float]:
    """
    Return the GBP -> currency rate for the given date.
    Tries exact match first, then searches within ±tolerance_days.

    Returns None if no rate is found within the tolerance window.
    """

    if currency == "GBP":
        return 1.0
    
    with get_conn() as conn:
        cursor = conn.cursor()

        cursor.execute(
            "SELECT rate FROM fx_rates WHERE target_currency = ? AND date = ?",
            (currency, on_date.isoformat()),
        )
        row = cursor.fetchone()
        if row:
            return row[0]
        
        # Fall back to nearest date within tolerance
        for delta in range(1, tolerance_days + 1):
            for candidate in [on_date - timedelta(days=delta), on_date + timedelta(days=delta)]:
                cursor.execute(
                    "SELECT rate FROM fx_rates WHERE target_currency = ? AND date = ?",
                    (currency, candidate.isoformat())
                )
                row = cursor.fetchone()
                if row:
                    return row[0]
        return None

def convert_to_gbp(amount: float, currency: str, on_date: date, tolerance_days: int = 7) -> Optional[float]:
    """Convert amount in currency to GBP using the stored FX rate for on_date.

    Falls back to the nearest available rate within tolerance_days if an exact
    date match is unavailable.  Returns None if no rate is found.
    """
    if currency == "GBP":
        return round(amount, 6)
    rate = get_gbp_rate(currency, on_date, tolerance_days)
    if rate is None or rate == 0:
        return None
    return round(amount / rate, 6)

def insert_fx_json(quotes: dict):
    """
    Insert FX rates from a timeframe API response.
    :param quotes: dict of date -> {currency_pair -> rate} (e.g. {"2026-03-01": {"GBPUSD": 1.25, ...}})
    """
    for date, pairs in quotes.items():
        for source_target_pair, rate in pairs.items():
            source_currency = source_target_pair[:3]
            target_currency = source_target_pair[3:]

            if source_currency != SOURCE_CURRENCY:
                logger.warning(f"Unexpected source currency '{source_currency}' in FX data for {date} (expected {SOURCE_CURRENCY})")
                continue

            insert_fx_rate(
                date=date,
                source_currency=source_currency,
                target_currency=target_currency,
                rate=float(rate),
                ts=int(datetime.strptime(date, "%Y-%m-%d").timestamp()),
            )

def insert_fx_file(fx_path: str) -> None:
    """Load a JSON backup file and insert all FX rates it contains."""
    with open(fx_path) as f:
        fx_data = json.load(f)
    for fx in fx_data:
        insert_fx_json(fx)

def get_api_usage(service: str = "exchangerate.host") -> dict:
    """Return current usage count and month for a service."""
    with get_conn(read_only=True) as conn:
        row = conn.execute(
            "SELECT count, month FROM api_usage WHERE service = ?", (service,)
        ).fetchone()
    return {"service": service, "count": row["count"], "month": row["month"]} if row else {"service": service, "count": 0, "month": None}


def reset_api_usage(service: str = "exchangerate.host"):
    """Reset the API usage count to 0 for a service."""
    month = datetime.now().strftime("%Y-%m")
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM api_usage WHERE service = ?", (service))
        old_service, old_count, old_month = row.fetchone()

        conn.execute("""
            INSERT INTO api_usage (service, count, month)
            VALUES (?, 0, ?)
            ON CONFLICT(service) DO UPDATE SET count = 0, month = ?
        """, (service, month, month))
    logger.info(f"Reset API usage for {service} (month: {month})")
    return {
        "service": service if service == old_service else old_service,
        "old_count": old_count,
        "old_month": old_month
    }


def set_api_usage(service: str = "exchangerate.host", count: int = 0):
    """Manually set the API usage count — use to initialise or correct the count."""
    month = datetime.now().strftime("%Y-%m")
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO api_usage (service, count, month)
            VALUES (?, ?, ?)
            ON CONFLICT(service) DO UPDATE SET count = ?, month = ?
        """, (service, count, month, count, month))
    logger.info(f"Set API usage for {service} to {count} (month: {month})")