"""
metadata/uploads.py
~~~~~~~~~~~~~~~~~~~~
Upload recency helpers for the metadata dashboard endpoint.

Provides:
  - get_last_uploads()    — most recent timestamp per data source
  - get_fx_latest_date()  — most recent date for which FX rates are stored
"""

from database.connection import get_conn


def get_last_uploads() -> dict:
    """Return the most recent timestamp for each data source."""
    with get_conn(read_only=True) as conn:
        def latest(query: str) -> str | None:
            row = conn.execute(query).fetchone()
            return row[0] if row and row[0] else None

        return {
            "location_shortcuts": latest(
                "SELECT MAX(timestamp) FROM location_shortcuts"
            ),
            "location_overland": latest(
                "SELECT MAX(timestamp) FROM location_overland"
            ),
            "health": latest(
                "SELECT MAX(timestamp) FROM health_quantity"
            ),
            "transactions": latest(
                "SELECT MAX(timestamp) FROM transactions"
            ),
            "fx_rates": latest(
                "SELECT MAX(date) FROM fx_rates"
            ),
            "workouts": latest(
                "SELECT MAX(end_ts) FROM workouts"
            ),
        }


def get_fx_latest_date() -> str | None:
    """Return the most recent date for which FX rates are stored."""
    with get_conn(read_only=True) as conn:
        row = conn.execute("SELECT MAX(date) FROM fx_rates").fetchone()
        return row[0] if row and row[0] else None
