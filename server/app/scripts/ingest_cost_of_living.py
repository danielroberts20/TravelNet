"""
scripts/ingest_cost_of_living.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
One-off script to bulk-populate the cost_of_living table with Numbeo data.
Re-run annually to refresh figures — INSERT OR REPLACE will update existing rows.

Source: https://www.numbeo.com/cost-of-living/rankings_by_country.jsp
        https://www.numbeo.com/cost-of-living/rankings.jsp (city-level)

Baseline: New York City = 100.0 for all indices.

center_lat / center_lon are set for city-level entries only and are used
for geofence-based CoL matching at query time. Country-level entries have
NULL coordinates and serve as fallbacks when no city geofence matches.

HOW TO UPDATE:
  1. Visit https://www.numbeo.com/cost-of-living/rankings_by_country.jsp?title=<year>
  2. Visit https://www.numbeo.com/cost-of-living/rankings.jsp (city-level)
  3. Update REFERENCE_YEAR and fill in updated index values below.
  4. Run: python -m scripts.ingest_cost_of_living
"""

from database.connection import get_conn

REFERENCE_YEAR = 2026
SOURCE = f"Numbeo {REFERENCE_YEAR}"

ENTRIES = [

    # UNITED STATES
    dict(country_code="US", country="United States", city="",
         col_index=68.8, rent_index=40.7, col_plus_rent=56.3,
         groceries_index=74.0, restaurant_index=72.8,
         local_currency="USD", is_estimated=False, notes=None,
         center_lat=None, center_lon=None),
    dict(country_code="US", country="United States", city="Philadelphia",
         col_index=78.8, rent_index=44.7, col_plus_rent=63.6,
         groceries_index=84.2, restaurant_index=78.6,
         local_currency="USD", is_estimated=False, notes=None,
         center_lat=39.9526, center_lon=-75.1652),
    dict(country_code="US", country="United States", city="Seattle",
         col_index=90.3, rent_index=60.2, col_plus_rent=76.9,
         groceries_index=98.0, restaurant_index=95.8,
         local_currency="USD", is_estimated=False, notes=None,
         center_lat=47.6062, center_lon=-122.3321),

    # FIJI
    dict(country_code="FJ", country="Fiji", city="",
         col_index=34.3, rent_index=17.2, col_plus_rent=26.7,
         groceries_index=43.4, restaurant_index=32.5,
         local_currency="FJD", is_estimated=False, notes=None,
         center_lat=None, center_lon=None),

    # AUSTRALIA
    dict(country_code="AU", country="Australia", city="",
         col_index=67.9, rent_index=33.7, col_plus_rent=52.7,
         groceries_index=76.5, restaurant_index=65.6,
         local_currency="AUD", is_estimated=False, notes=None,
         center_lat=None, center_lon=None),
    dict(country_code="AU", country="Australia", city="Sydney",
         col_index=75.1, rent_index=53.8, col_plus_rent=65.6,
         groceries_index=79.2, restaurant_index=69.6,
         local_currency="AUD", is_estimated=False, notes=None,
         center_lat=-33.8688, center_lon=151.2093),
    dict(country_code="AU", country="Australia", city="Melbourne",
         col_index=70.8, rent_index=39.7, col_plus_rent=56.9,
         groceries_index=79.5, restaurant_index=66.7,
         local_currency="AUD", is_estimated=False, notes=None,
         center_lat=-37.8136, center_lon=144.9631),
    dict(country_code="AU", country="Australia", city="Brisbane",
         col_index=64.8, rent_index=38.5, col_plus_rent=53.1,
         groceries_index=76.6, restaurant_index=66.5,
         local_currency="AUD", is_estimated=False, notes=None,
         center_lat=-27.4698, center_lon=153.0251),

    # NEW ZEALAND
    dict(country_code="NZ", country="New Zealand", city="",
         col_index=60.3, rent_index=26.2, col_plus_rent=45.0,
         groceries_index=65.4, restaurant_index=59.3,
         local_currency="NZD", is_estimated=False, notes=None,
         center_lat=None, center_lon=None),
    dict(country_code="NZ", country="New Zealand", city="Auckland",
         col_index=62.5, rent_index=31.6, col_plus_rent=48.7,
         groceries_index=67.0, restaurant_index=59.4,
         local_currency="NZD", is_estimated=False, notes=None,
         center_lat=-36.8509, center_lon=174.7645),

    # THAILAND
    dict(country_code="TH", country="Thailand", city="",
         col_index=38.0, rent_index=13.9, col_plus_rent=27.2,
         groceries_index=44.4, restaurant_index=25.0,
         local_currency="THB", is_estimated=False, notes=None,
         center_lat=None, center_lon=None),
    dict(country_code="TH", country="Thailand", city="Bangkok",
         col_index=41.4, rent_index=19.6, col_plus_rent=31.6,
         groceries_index=45.2, restaurant_index=29.2,
         local_currency="THB", is_estimated=False, notes=None,
         center_lat=13.7563, center_lon=100.5018),

    # VIETNAM
    dict(country_code="VN", country="Vietnam", city="",
         col_index=26.4, rent_index=9.9, col_plus_rent=19.1,
         groceries_index=31.8, restaurant_index=15.6,
         local_currency="VND", is_estimated=False, notes=None,
         center_lat=None, center_lon=None),
    dict(country_code="VN", country="Vietnam", city="Hanoi",
         col_index=28.4, rent_index=9.8, col_plus_rent=20.1,
         groceries_index=34.1, restaurant_index=18.5,
         local_currency="VND", is_estimated=False, notes=None,
         center_lat=21.0285, center_lon=105.8542),
    dict(country_code="VN", country="Vietnam", city="Ho Chi Minh City",
         col_index=28.2, rent_index=12.9, col_plus_rent=21.4,
         groceries_index=32.4, restaurant_index=16.7,
         local_currency="VND", is_estimated=False, notes=None,
         center_lat=10.8231, center_lon=106.6297),

    # MALAYSIA
    dict(country_code="MY", country="Malaysia", city="",
         col_index=34.0, rent_index=9.2, col_plus_rent=22.9,
         groceries_index=42.0, restaurant_index=25.2,
         local_currency="MYR", is_estimated=False, notes=None,
         center_lat=None, center_lon=None),
    dict(country_code="MY", country="Malaysia", city="Kuala Lumpur",
         col_index=37.4, rent_index=13.8, col_plus_rent=26.9,
         groceries_index=44.7, restaurant_index=29.6,
         local_currency="MYR", is_estimated=False, notes=None,
         center_lat=3.1390, center_lon=101.6869),

    # SINGAPORE
    dict(country_code="SG", country="Singapore", city="",
         col_index=87.7, rent_index=73.1, col_plus_rent=81.2,
         groceries_index=77.3, restaurant_index=55.5,
         local_currency="SGD", is_estimated=False, notes=None,
         center_lat=None, center_lon=None),

    # INDONESIA
    dict(country_code="ID", country="Indonesia", city="",
         col_index=26.1, rent_index=9.1, col_plus_rent=18.5,
         groceries_index=33.6, restaurant_index=15.3,
         local_currency="IDR", is_estimated=False, notes=None,
         center_lat=None, center_lon=None),
    dict(country_code="ID", country="Indonesia", city="Bali",
         col_index=37.3, rent_index=32.8, col_plus_rent=35.3,
         groceries_index=49.1, restaurant_index=22.9,
         local_currency="IDR", is_estimated=False,
         notes="Numbeo city name: 'Denpasar (Bali)'",
         center_lat=-8.6705, center_lon=115.2126),

    # CAMBODIA
    dict(country_code="KH", country="Cambodia", city="",
         col_index=34.8, rent_index=10.0, col_plus_rent=23.7,
         groceries_index=41.6, restaurant_index=25.4,
         local_currency="USD", is_estimated=False, notes=None,
         center_lat=None, center_lon=None),
    dict(country_code="KH", country="Cambodia", city="Phnom Penh",
         col_index=37.9, rent_index=13.7, col_plus_rent=27.1,
         groceries_index=44.6, restaurant_index=31.1,
         local_currency="USD", is_estimated=False, notes=None,
         center_lat=11.5564, center_lon=104.9282),

    # CANADA
    dict(country_code="CA", country="Canada", city="",
         col_index=63.0, rent_index=31.5, col_plus_rent=48.9,
         groceries_index=69.6, restaurant_index=64.1,
         local_currency="CAD", is_estimated=False, notes=None,
         center_lat=None, center_lon=None),
    dict(country_code="CA", country="Canada", city="Vancouver",
         col_index=67.5, rent_index=48.0, col_plus_rent=58.8,
         groceries_index=76.1, restaurant_index=69.0,
         local_currency="CAD", is_estimated=False, notes=None,
         center_lat=49.2827, center_lon=-123.1207),

    # UNITED KINGDOM
    dict(country_code="GB", country="United Kingdom", city="",
         col_index=67.8, rent_index=32.1, col_plus_rent=51.9,
         groceries_index=62.8, restaurant_index=72.9,
         local_currency="GBP", is_estimated=False, notes=None,
         center_lat=None, center_lon=None),
    dict(country_code="GB", country="United Kingdom", city="London",
         col_index=87.5, rent_index=70.1, col_plus_rent=79.7,
         groceries_index=68.9, restaurant_index=89.7,
         local_currency="GBP", is_estimated=False,
         notes="UK home base — included as a reference point.",
         center_lat=51.5074, center_lon=-0.1278),
]


SQL = """
    INSERT OR REPLACE INTO cost_of_living (
        country_code, country, city,
        col_index, rent_index, col_plus_rent,
        groceries_index, restaurant_index,
        local_currency, source, reference_year,
        is_estimated, notes,
        center_lat, center_lon
    ) VALUES (
        :country_code, :country, :city,
        :col_index, :rent_index, :col_plus_rent,
        :groceries_index, :restaurant_index,
        :local_currency, :source, :reference_year,
        :is_estimated, :notes,
        :center_lat, :center_lon
    )
"""


def run() -> None:
    rows = [{**e, "source": SOURCE, "reference_year": REFERENCE_YEAR} for e in ENTRIES]
    with get_conn() as conn:
        conn.executemany(SQL, rows)
    estimated = sum(1 for r in rows if r["is_estimated"])
    print(
        f"Inserted/updated {len(rows)} rows "
        f"({estimated} estimated, {len(rows) - estimated} from Numbeo). "
        f"Source: {SOURCE}."
    )


if __name__ == "__main__":
    run()