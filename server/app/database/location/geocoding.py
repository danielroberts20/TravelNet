"""
database/location/geocoding.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Nominatim reverse-geocoding helpers and place-id lookup/creation.

get_place_id()   — upsert a places row and return its id (used at every
                   location insert point to snap lat/lon to the nearest
                   0.001° grid cell).
insert_geocode() — update a places row with a geocoding result from
                   reverse_geocode(). Derives `locality` and `region`
                   from Nominatim's country-dependent address fields
                   using a preference chain, and stores the full raw
                   response for future re-processing.
reverse_geocode() — call the Nominatim API for a single lat/lon.
batch_geocode()   — geocode a list of coordinates with a 1s rate-limit delay.
"""
from datetime import datetime, timezone
import json
import logging
import sqlite3
import time

import requests

from database.connection import get_conn, to_iso_str

logger = logging.getLogger(__name__)

GEOCODE_BACKOFF_BASE_S = 60
GEOCODE_BACKOFF_MAX_S  = 480
GEOCODE_MAX_RETRIES    = 4


# ---------------------------------------------------------------------------
# Preference chains for Nominatim's variable address structure
# ---------------------------------------------------------------------------
#
# Nominatim's `address` dict varies by country. A UK town returns
# {"town": "Shaftesbury"} with no `city` key; a US city returns
# {"city": "Seattle"}; Japan returns {"city": "Osaka", "city_district": "Kita"};
# Russia returns {"state": "...", "federal_state": "..."}.
#
# Rather than trying to fit all variants into fixed columns, we pick the
# single best value for "locality" (what a human would call this place)
# and "region" (the broader administrative area) from an ordered list
# of fallbacks. The full raw response is stored in places.raw_json so the
# preference chain can be revised without re-geocoding.
# ---------------------------------------------------------------------------

_LOCALITY_PREFERENCE = (
    "city",
    "town",
    "village",
    "municipality",
    "hamlet",
    "suburb",
    "neighbourhood",
    "city_district",
    "county",
)

_REGION_PREFERENCE = (
    "state",
    "province",
    "region",
    "state_district",
    "county",
)


def _pick(address: dict, keys: tuple[str, ...]) -> str | None:
    """Return the first non-empty value for any of `keys` in `address`."""
    for k in keys:
        v = address.get(k)
        if v:
            return v
    return None


# ---------------------------------------------------------------------------
# Nominatim API calls
# ---------------------------------------------------------------------------

def reverse_geocode(lat: float, lon: float) -> dict | None:
    for attempt in range(GEOCODE_MAX_RETRIES + 1):
        try:
            response = requests.get(
                "https://nominatim.openstreetmap.org/reverse",
                params={"lat": lat, "lon": lon, "format": "jsonv2"},
                headers={"User-Agent": "TravelNet/1.0 dan@travelnet.dev"},  # required
                timeout=10,
            )
            if response.status_code == 429:
                if attempt < GEOCODE_MAX_RETRIES:
                    wait = min(GEOCODE_BACKOFF_BASE_S * (2 ** attempt), GEOCODE_BACKOFF_MAX_S)
                    logger.warning(
                        f"Nominatim 429 for ({lat}, {lon}). "
                        f"Attempt {attempt + 1}/{GEOCODE_MAX_RETRIES + 1}. "
                        f"Waiting {wait}s."
                    )
                    time.sleep(wait)
                    continue
                else:
                    logger.warning(
                        f"Nominatim 429 for ({lat}, {lon}) after "
                        f"{GEOCODE_MAX_RETRIES + 1} attempts. Skipping."
                    )
                    return None
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            if attempt < GEOCODE_MAX_RETRIES:
                wait = min(GEOCODE_BACKOFF_BASE_S * (2 ** attempt), GEOCODE_BACKOFF_MAX_S)
                logger.warning(
                    f"Nominatim request error for ({lat}, {lon}): {exc}. "
                    f"Attempt {attempt + 1}/{GEOCODE_MAX_RETRIES + 1}. "
                    f"Waiting {wait}s."
                )
                time.sleep(wait)
            else:
                logger.warning(
                    f"Nominatim failed for ({lat}, {lon}) after "
                    f"{GEOCODE_MAX_RETRIES + 1} attempts: {exc}. Skipping."
                )
                return None


def batch_geocode(coords: list[tuple[float, float]]) -> list[dict]:
    locations = []
    errors = []
    for lat, lon in coords:
        loc = reverse_geocode(lat, lon)
        if loc is not None:
            locations.append(loc)
            time.sleep(1.5)
        else:
            errors.append({"lat": lat, "lon": lon, "error": "geocoding failed after retries"})
    return locations, errors


# ---------------------------------------------------------------------------
# DB writes
# ---------------------------------------------------------------------------

def insert_geocode(place_id: int, geocode: dict, conn=None) -> None:
    """
    Update a places row with a Nominatim reverse-geocode result.

    Populates denormalised columns (country, city, suburb, road) from
    the address dict, derives `locality` and `region` via the preference
    chains, and stores the full response as JSON for future reference.
    """
    addr = geocode.get("address") or {}

    locality = _pick(addr, _LOCALITY_PREFERENCE)
    region   = _pick(addr, _REGION_PREFERENCE)

    params = (
        addr.get("country_code"),
        addr.get("country"),
        region,
        locality,
        addr.get("city"),
        addr.get("suburb"),
        addr.get("road"),
        geocode.get("display_name"),
        json.dumps(geocode) if geocode is not None else None,
        to_iso_str(datetime.now(timezone.utc)),
        place_id,
    )
    sql = """
        UPDATE places SET
            country_code = ?,
            country      = ?,
            region       = ?,
            locality     = ?,
            city         = ?,
            suburb       = ?,
            road         = ?,
            display_name = ?,
            raw_json     = ?,
            geocoded_at  = ?
        WHERE id = ?
    """
    if conn is not None:
        conn.execute(sql, params)
    else:
        with get_conn() as c:
            c.execute(sql, params)


def get_place_id(lat: float | None, lon: float | None, conn: sqlite3.Connection = None) -> int | None:
    if lat is None or lon is None:
        return None
    
    lat_snap, lon_snap = round(lat, 3), round(lon, 3)
    conn = conn if conn else get_conn()
    with conn:
        conn.execute(
            "INSERT OR IGNORE INTO places (lat_snap, lon_snap) VALUES (?, ?)",
            (lat_snap, lon_snap),
        )
        row = conn.execute(
            "SELECT id FROM places WHERE lat_snap = ? AND lon_snap = ?",
            (lat_snap, lon_snap),
        ).fetchone()
    return row[0] if row else None