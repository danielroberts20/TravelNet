from datetime import date, timedelta
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
    if currency == "GBP":
        return round(amount, 6)
    rate = get_gbp_rate(currency, on_date, tolerance_days)
    if rate is None or rate == 0:
        return None
    return round(amount / rate, 6)

def insert_fx_json(fx_data: dict):
    quotes = fx_data.get("quotes", {})
    for source_target_pair in quotes.keys(): # Loop through returned FX rates (e.g. "GBPUSD", "GBPCAD" etc.)
        source_currency, target_currency = source_target_pair[:3], source_target_pair[3:] # Extract source and target currency from pair string
        if source_currency != SOURCE_CURRENCY: # Sanity check - should always be the same as config.SOURCE_CURRENCY
            logger.warning(f"Unexpected source currency in FX data: {source_currency} (expected {SOURCE_CURRENCY})")
            continue
        rate = quotes[source_target_pair] # Extract rate from response
        insert_fx_rate(
            date=fx_data.get("date"), 
            source_currency=source_currency, 
            target_currency=target_currency, 
            rate=float(rate),
            ts=int(fx_data.get("timestamp"))) # Insert into DB

def insert_fx_file(fx_path: str):
    with open(fx_path) as f:
        fx_data = json.load(f)
    for fx in fx_data:
        insert_fx_json(fx)