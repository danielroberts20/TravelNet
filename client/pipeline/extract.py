# client/pipeline/extract.py
"""
extract.py

Converts SQL -> DataFrame
"""

import sqlite3
import pandas as pd
from pathlib import Path
import requests
from client.config.paths import DB_PATH
from client.config.settings import UPLOAD_TOKEN, TRAVELNET_URL


def download_db(replace: bool = False):
    """
    Download DB from TravelNet
    :param replace: Whether to overwrite the database currently stored at ``DB_PATH``. Default is ``False``.
    :return:
    """
    if not DB_PATH.exists() or replace:
        response = requests.get(TRAVELNET_URL / "database" / "download",
                                headers={"Authorization": f"Bearer {UPLOAD_TOKEN}"})
        with open(DB_PATH, "wb") as f:
            f.write(response.content)

def get_conn() -> sqlite3.Connection:
    """Return a sqlite3 connection to the local DB snapshot."""
    return sqlite3.connect(DB_PATH)

def extract_location(conn) -> pd.DataFrame:
    """
    Pull all location records from the unified view.
    Returns one row per location ping with timestamp, coordinates,
    activity type, and source.
    """
    query = """
        SELECT
            timestamp,
            lat,
            lon,
            altitude,
            activity,
            battery,
            source,
            device
        FROM location_unified
        ORDER BY timestamp ASC
    """
    df = pd.read_sql_query(query, conn, parse_dates=["timestamp"])
    return df

def extract_transactions(conn) -> pd.DataFrame:
    query = """
        SELECT
            id,
            date,
            description,
            amount,
            currency,
            amount_gbp,
            category,
            source         -- 'revolut', 'wise', 'cash'
        FROM transactions
        ORDER BY date ASC
    """
    df = pd.read_sql_query(query, conn, parse_dates=["date"])
    return df

def extract_fx(conn) -> pd.DataFrame:
    query = """
        SELECT
            date,
            currency,
            rate_to_gbp
        FROM fx_rates
        ORDER BY date ASC
    """
    df = pd.read_sql_query(query, conn, parse_dates=["date"])
    return df

def extract_health(conn) -> pd.DataFrame:
    query = """
        SELECT
            date,
            metric,
            value,
            unit
        FROM health_data
        ORDER BY date ASC
    """
    df = pd.read_sql_query(query, conn, parse_dates=["date"])
    return df