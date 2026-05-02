"""
database/cost_of_living/queries.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Lookup helpers for the cost_of_living table.

get_col_entry()  — resolve the best CoL row for a given country + coordinates,
                   using a geofence match on city center coordinates with
                   country-level fallback. Also returns which index field to use.

get_uk_col_index() — fetch the UK country-level col_index, used as the
                     normalisation baseline for spend_normalised.
"""

import math
import sqlite3

from util import haversine_km
from database.location.geocoding import get_place_id
from config.general import COL_CITY_RADIUS_KM


# ---------------------------------------------------------------------------
# Main lookup
# ---------------------------------------------------------------------------

def get_col_entry(
    lat: float | None,
    lon: float | None,
    conn: sqlite3.Connection,
) -> dict | None:
    """
    Resolve the best cost_of_living row for a given country and coordinates.

    Resolution order:
      1. City-level: find all rows for country_code with center_lat/center_lon set,
         pick the nearest whose center is within COL_CITY_RADIUS_KM. If found,
         use col_plus_rent.
      2. Country-level fallback (city = ''): use col_plus_rent, EXCEPT for US
         where col is used (camp / non-accommodation scenario).

    Returns a dict with the matched row fields plus:
      - 'index_value': the resolved float to use for normalisation
      - 'city_matched': True if a city geofence matched, False if country fallback
      - 'index_field': 'col_plus_rent' or 'col_index' (for logging/debug)

    Returns None if no row exists for the country at all.
    """
    city_matched = False
    row = None

    place_id = get_place_id(lat, lon, conn=conn)
    if place_id is None:
            return None
    
    place = conn.execute("""
        SELECT country_code, lat_snap, lon_snap
        FROM places WHERE id = ? AND geocoded_at IS NOT NULL
    """, (place_id,)).fetchone()

    if place is None:
        return None
    
    country_code = place["country_code"]
    lat = place["lat_snap"]
    lon = place["lon_snap"]
    
    if country_code is None:
        return None

    # --- City geofence match ---
    if lat is not None and lon is not None:
        city_rows = conn.execute("""
            SELECT *
            FROM cost_of_living
            WHERE country_code = ?
              AND city != ''
              AND center_lat IS NOT NULL
              AND center_lon IS NOT NULL
        """, (country_code.upper(),)).fetchall()

        best_dist = None
        best_row  = None
        for r in city_rows:
            dist = haversine_km(lat, lon, r["center_lat"], r["center_lon"])
            if dist <= COL_CITY_RADIUS_KM:
                if best_dist is None or dist < best_dist:
                    best_dist = dist
                    best_row  = r

        if best_row is not None:
            row         = best_row
            city_matched = True

    # --- Country-level fallback ---
    if row is None:
        row = conn.execute("""
            SELECT * FROM cost_of_living
            WHERE country_code = ? AND city = ''
            LIMIT 1
        """, (country_code.upper(),)).fetchone()

    if row is None:
        return None

    # --- Index selection ---
    # City match → always col_plus_rent.
    # Country fallback → col_plus_rent except US (camp = no paid accommodation).
    if city_matched:
        index_field = "col_plus_rent"
        index_value = row["col_plus_rent"]
    elif country_code == "US":
        index_field = "col_index"
        index_value = row["col_index"]
    else:
        index_field = "col_plus_rent"
        index_value = row["col_plus_rent"]

    return {
        **dict(row),
        "index_value":  index_value,
        "index_field":  index_field,
        "city_matched": city_matched,
    }


# ---------------------------------------------------------------------------
# UK baseline
# ---------------------------------------------------------------------------

_UK_COL_CACHE: float | None = None


def get_uk_col_index(conn: sqlite3.Connection) -> float | None:
    """
    Return the UK country-level col_index, used as the normalisation baseline.
    Result is module-level cached after first DB read (value rarely changes).
    """
    global _UK_COL_CACHE
    if _UK_COL_CACHE is not None:
        return _UK_COL_CACHE

    row = conn.execute("""
        SELECT col_index FROM cost_of_living
        WHERE country_code = 'GB' AND city = ''
        LIMIT 1
    """).fetchone()

    if row and row["col_index"]:
        _UK_COL_CACHE = row["col_index"]

    return _UK_COL_CACHE