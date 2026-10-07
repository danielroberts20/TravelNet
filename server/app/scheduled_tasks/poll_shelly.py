"""
poll_shelly.py
~~~~~~~~~~~~~~
Polls the Shelly smart plug for current power draw and upserts a daily
aggregate into power_daily.

Runs every 5 minutes via Prefect. The upsert pattern means the daily row
is updated on every run — if the Pi restarts mid-day, no data is lost.

This flow fires ~288 times a day, so it is deliberately lean: plain functions
instead of Prefect tasks (a task run costs three state rows plus log lines),
the read-modify-write happens in one transaction (see
PowerDailyTable.upsert_reading), and the happy path logs at DEBUG so successful
runs ship nothing to the Prefect API log. Failures still log and notify.
"""

from datetime import datetime, timezone

import requests
from prefect import flow, get_run_logger

from config.settings import settings
from database.power.table import table as power_table
from notifications import notify_on_completion

SHELLY_TIMEOUT = 5


def fetch_shelly_reading() -> dict:
    """Fetch current wattage and cumulative energy from the Shelly local API.

    Raises on any failure (network, bad JSON, missing keys); the flow decides
    how to handle it.
    """
    resp = requests.post(
        f"http://{settings.shelly_ip}/rpc/Switch.GetStatus",
        json={"id": 0},
        timeout=SHELLY_TIMEOUT,
    )
    data = resp.json()
    return {
        "apower": float(data["apower"]),
        "aenergy_total": float(data["aenergy"]["total"]),
    }


@flow(name="Get Power Statistics", on_failure=[notify_on_completion])
def poll_shelly_flow():
    log = get_run_logger()

    try:
        reading = fetch_shelly_reading()
    except Exception as e:
        log.warning(f"Failed to fetch Shelly reading: {e}")
        log.warning("No reading obtained — skipping upsert.")
        return

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    record = power_table.upsert_reading(today, reading["apower"], reading["aenergy_total"])
    log.debug(
        f"Power: min={record.min_w}W max={record.max_w}W avg={record.avg_w}W "
        f"total={record.total_wh}Wh readings={record.readings}"
    )
