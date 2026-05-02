"""
upload/cost_of_living/router.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Upload endpoint for cost_of_living rows.
Intended for admin use via the Dashboard form — no auth required
(Dashboard is Tailscale-only).
"""

from fastapi import APIRouter
from pydantic import BaseModel, field_validator, model_validator
from typing import Optional

from database.connection import get_conn

router = APIRouter()


class CostOfLivingEntry(BaseModel):
    country_code: str
    country: str
    city: str = ""                     # empty string = country-level entry
    center_lat: Optional[float] = None
    center_lon: Optional[float] = None
    col_index: Optional[float] = None
    rent_index: Optional[float] = None
    col_plus_rent: Optional[float] = None
    groceries_index: Optional[float] = None
    restaurant_index: Optional[float] = None
    local_currency: str
    source: str
    reference_year: int
    is_estimated: bool = False
    notes: Optional[str] = None

    @field_validator("country_code")
    @classmethod
    def validate_country_code(cls, v: str) -> str:
        v = v.strip().upper()
        if len(v) != 2:
            raise ValueError("country_code must be a 2-letter ISO 3166-1 alpha-2 code")
        return v

    @field_validator("local_currency")
    @classmethod
    def validate_currency(cls, v: str) -> str:
        v = v.strip().upper()
        if len(v) != 3:
            raise ValueError("local_currency must be a 3-letter ISO 4217 code")
        return v

    @field_validator("reference_year")
    @classmethod
    def validate_year(cls, v: int) -> int:
        if not (2020 <= v <= 2100):
            raise ValueError("reference_year must be a plausible year (2020–2100)")
        return v

    @field_validator(
        "col_index", "rent_index", "col_plus_rent",
        "groceries_index", "restaurant_index",
        mode="before",
    )
    @classmethod
    def validate_index(cls, v):
        if v is None:
            return v
        if not (0 <= float(v) <= 200):
            raise ValueError("Index values must be between 0 and 200 (NYC = 100 baseline)")
        return float(v)

    @field_validator("center_lat", mode="before")
    @classmethod
    def validate_lat(cls, v):
        if v is None:
            return v
        if not (-90 <= float(v) <= 90):
            raise ValueError("center_lat must be between -90 and 90")
        return float(v)

    @field_validator("center_lon", mode="before")
    @classmethod
    def validate_lon(cls, v):
        if v is None:
            return v
        if not (-180 <= float(v) <= 180):
            raise ValueError("center_lon must be between -180 and 180")
        return float(v)

    @model_validator(mode="after")
    def validate_entry(self) -> "CostOfLivingEntry":
        if self.col_index is None and not self.is_estimated:
            raise ValueError(
                "col_index is required for non-estimated entries. "
                "Set is_estimated=true if this is a proxy value."
            )
        if (self.center_lat is None) != (self.center_lon is None):
            raise ValueError("center_lat and center_lon must both be set or both be null")
        return self


class CostOfLivingResponse(BaseModel):
    status: str                # "inserted" or "updated"
    country_code: str
    country: str
    city: str
    center_lat: Optional[float]
    center_lon: Optional[float]
    col_index: Optional[float]
    rent_index: Optional[float]
    col_plus_rent: Optional[float]
    groceries_index: Optional[float]
    restaurant_index: Optional[float]
    local_currency: str
    source: str
    reference_year: int
    is_estimated: bool
    notes: Optional[str]


SQL_CHECK = """
    SELECT id FROM cost_of_living
    WHERE country_code = ? AND city = ?
"""

SQL_UPSERT = """
    INSERT INTO cost_of_living (
        country_code, country, city,
        center_lat, center_lon,
        col_index, rent_index, col_plus_rent,
        groceries_index, restaurant_index,
        local_currency, source, reference_year,
        is_estimated, notes
    ) VALUES (
        :country_code, :country, :city,
        :center_lat, :center_lon,
        :col_index, :rent_index, :col_plus_rent,
        :groceries_index, :restaurant_index,
        :local_currency, :source, :reference_year,
        :is_estimated, :notes
    )
    ON CONFLICT(country_code, city) DO UPDATE SET
        country          = excluded.country,
        center_lat       = excluded.center_lat,
        center_lon       = excluded.center_lon,
        col_index        = excluded.col_index,
        rent_index       = excluded.rent_index,
        col_plus_rent    = excluded.col_plus_rent,
        groceries_index  = excluded.groceries_index,
        restaurant_index = excluded.restaurant_index,
        local_currency   = excluded.local_currency,
        source           = excluded.source,
        reference_year   = excluded.reference_year,
        is_estimated     = excluded.is_estimated,
        notes            = excluded.notes
"""


@router.post("", response_model=CostOfLivingResponse)
def upload_cost_of_living(entry: CostOfLivingEntry) -> CostOfLivingResponse:
    with get_conn() as conn:
        existing = conn.execute(
            SQL_CHECK, (entry.country_code, entry.city)
        ).fetchone()

        conn.execute(SQL_UPSERT, entry.model_dump())

        row = conn.execute(
            "SELECT * FROM cost_of_living WHERE country_code = ? AND city = ?",
            (entry.country_code, entry.city),
        ).fetchone()

    return CostOfLivingResponse(
        status="updated" if existing else "inserted",
        **{k: row[k] for k in CostOfLivingResponse.model_fields if k != "status"},
    )