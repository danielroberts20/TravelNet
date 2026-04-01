"""
database/transaction/ingest/util.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Shared helpers used by both the Revolut and Wise ingest modules.
"""

from typing import Optional


def safe_float(value: str) -> Optional[float]:
    """Parse a string to float, returning None for blank or unparseable values."""
    try:
        return float(value) if value.strip() != "" else None
    except (ValueError, AttributeError):
        return None
