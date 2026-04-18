"""
poll_shelly.py
~~~~~~~~~~~~~~
Polls the Shelly smart plug for current power draw, accumulates readings
in memory, and upserts a daily aggregate into power_daily.

Runs every 5 minutes via Prefect. The upsert pattern means the daily row
is updated on every run — if the Pi restarts mid-day, no data is lost.
"""

from prefect import flow, task, get_run_logger
from datetime import datetime, timezone
import requests

from config.settings import settings
from database.power.table import table as power_table, PowerDailyRecord

SHELLY_TIMEOUT = 5


@task
def fetch_shelly_reading() -> float | None:
    """Fetch current wattage from Shelly local API."""
    log = get_run_logger()
    try:
        resp = requests.post(
            f"http://{settings.shelly_ip}/rpc/Switch.GetStatus",
            json={"id": 0},
            timeout=SHELLY_TIMEOUT,
        )
        data = resp.json()
        watts = float(data["apower"])
        log.info(f"Shelly reading: {watts}W")
        return {"apower": float(data["apower"]), "aenergy_total": float(data["aenergy"]["total"])}
    except Exception as e:
        log.warning(f"Failed to fetch Shelly reading: {e}")
        return None


@task
def get_todays_aggregate() -> dict | None:
    """Fetch today's existing aggregate from DB if it exists."""
    from database.connection import get_conn
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    with get_conn(read_only=True) as conn:
        row = conn.execute("""
            SELECT min_w, max_w, avg_w, readings, start_wh
            FROM power_daily
            WHERE date = ?
        """, (today,)).fetchone()
        if row:
            return dict(row)
        return None


@task
def upsert_aggregate(reading: dict, existing: dict | None) -> None:
    log = get_run_logger()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    watts = reading["apower"]
    energy_total = reading["aenergy_total"]

    if existing is None:
        new_min = watts
        new_max = watts
        new_avg = watts
        new_readings = 1
        start_wh = energy_total
    else:
        n = existing["readings"]
        new_min = min(existing["min_w"], watts)
        new_max = max(existing["max_w"], watts)
        new_avg = (existing["avg_w"] * n + watts) / (n + 1)
        new_readings = n + 1
        start_wh = existing["start_wh"]

    record = PowerDailyRecord(
        date=today,
        min_w=round(new_min, 2),
        max_w=round(new_max, 2),
        avg_w=round(new_avg, 2),
        readings=new_readings,
        start_wh=start_wh,
        end_wh=energy_total,
    )
    power_table.insert(record)
    log.info(f"Power: min={new_min}W max={new_max}W avg={round(new_avg,2)}W total={record.total_wh}Wh")


@flow(name="Get Power Statistics")
def poll_shelly_flow():
    log = get_run_logger()
    reading = fetch_shelly_reading()
    if reading is None:
        log.warning("No reading obtained — skipping upsert.")
        return
    existing = get_todays_aggregate()
    upsert_aggregate(reading, existing)