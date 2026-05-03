from config.editable import load_overrides
load_overrides()

from datetime import datetime, timedelta, timezone
import json
import logging
import requests

from prefect import task, flow

from config.general import CURRENCIES, FX_BACKUP_DIR, FX_DATE_URL, SOURCE_CURRENCY
from config.settings import settings
from database.connection import increment_api_usage
from database.exchange.fx import insert_fx_json
from notifications import notify_on_completion, record_flow_result

logger = logging.getLogger(__name__)


def _extract_quotes(response: dict) -> dict:
    return response.get("quotes") or response.get("rates") or {}


@task(retries=3, retry_delay_seconds=5)
def fetch_fx_for_date(date_string: str, currencies: list[str] = None, source: str = SOURCE_CURRENCY) -> dict:
    if not currencies:
        currencies = CURRENCIES
    datetime.strptime(date_string, "%Y-%m-%d")

    params = {
        "access_key": settings.fx_api_key,
        "date": date_string,
        "source": source,
        "currencies": ",".join(currencies),
    }

    response = requests.get(FX_DATE_URL, params=params, timeout=30)
    response.raise_for_status()
    increment_api_usage("exchangerate.host")

    data = response.json()
    if data.get("success") is not True:
        raise RuntimeError(f"FX API error: {data.get('error')}")

    return data


@task
def store_fx(date_string: str, quotes: dict):
    insert_fx_json({date_string: quotes})


@task
def backup_fx(response: dict, path) -> str:
    with open(path, "w") as f:
        json.dump(response, f, indent=2)
    return str(path)


@flow(name="Get FX", on_failure=[notify_on_completion])
def get_fx_flow(date: datetime | None = None):
    if date is None:
        date = datetime.now(timezone.utc) - timedelta(days=2)

    date_string = date.strftime("%Y-%m-%d")

    response = fetch_fx_for_date(date_string, CURRENCIES, SOURCE_CURRENCY)
    quotes = _extract_quotes(response)

    store_fx(date_string, quotes)

    backup_path = FX_BACKUP_DIR / f"{date_string}.json"
    backup_fx(response, backup_path)

    result = {
        "date": date_string,
        "count": len(quotes),
        "backup": str(backup_path),
    }
    record_flow_result(result)
    return result