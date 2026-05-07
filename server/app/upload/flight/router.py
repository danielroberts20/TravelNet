"""
upload/flight/router.py
~~~~~~~~~~~~~~~~~~~~~~~
Manual flight entry endpoint.
Duration and distance are computed server-side from timestamps and
airportsdata. City/country are auto-populated from the library unless
the caller explicitly provides them.

Timestamps may be supplied as:
  - UTC-aware ISO 8601:  "2026-09-12T23:55:00Z" or "2026-09-12T23:55:00+05:30"
  - Naive local time:    "2026-09-12T23:55" or "2026-09-12T23:55:00"
    departed_at is interpreted as local time at the origin airport.
    arrived_at  is interpreted as local time at the destination airport.
    Timezone is determined from the airport's coordinates via timezonefinder.
"""

from datetime import datetime, timezone as dt_timezone
from zoneinfo import ZoneInfo

import airportsdata  # type: ignore
from fastapi import APIRouter, Depends, HTTPException  # type: ignore
from pydantic import BaseModel, field_validator  # type: ignore
from timezonefinder import TimezoneFinder  # type: ignore

from auth import require_upload_token
from database.flights.table import table as flights_table, FlightRecord
from util import haversine_km

router = APIRouter()

_airports = airportsdata.load("IATA")
_tf = TimezoneFinder()


def _get_airport(iata: str) -> dict | None:
    return _airports.get(iata.upper())



class FlightUpload(BaseModel):
    origin_iata: str
    destination_iata: str
    departed_at: str            # UTC or naive local at origin,  e.g. "2026-09-12T23:55"
    arrived_at: str             # UTC or naive local at destination
    origin_city: str | None = None
    destination_city: str | None = None
    origin_country: str | None = None
    destination_country: str | None = None
    airline: str | None = None
    flight_number: str | None = None
    seat_class: str | None = None
    notes: str | None = None

    @field_validator("origin_iata", "destination_iata")
    @classmethod
    def upper_iata(cls, v: str) -> str:
        return v.upper().strip()

    @field_validator("seat_class")
    @classmethod
    def validate_seat_class(cls, v: str | None) -> str | None:
        if v is not None and v not in ("economy", "premium_economy", "business"):
            raise ValueError("seat_class must be economy, premium_economy, or business")
        return v


def _to_utc(ts: str, lat: float, lon: float) -> datetime:
    """Parse a timestamp to UTC. Naive strings are treated as local time at (lat, lon)."""
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        tz_name = _tf.timezone_at(lat=lat, lng=lon)
        if tz_name is None:
            raise HTTPException(status_code=422, detail=f"Could not determine timezone at {lat},{lon}")
        dt = dt.replace(tzinfo=ZoneInfo(tz_name))
    return dt.astimezone(dt_timezone.utc)


@router.post("", dependencies=[Depends(require_upload_token)])
def upload_flight(payload: FlightUpload):
    o = _get_airport(payload.origin_iata)
    d = _get_airport(payload.destination_iata)

    if o is None:
        raise HTTPException(status_code=422, detail=f"Unknown IATA code: {payload.origin_iata}")
    if d is None:
        raise HTTPException(status_code=422, detail=f"Unknown IATA code: {payload.destination_iata}")

    departed = _to_utc(payload.departed_at, o["lat"], o["lon"])
    arrived  = _to_utc(payload.arrived_at,  d["lat"], d["lon"])
    duration_mins = round((arrived - departed).total_seconds() / 60)
    distance_km = round(haversine_km(o["lat"], o["lon"], d["lat"], d["lon"]), 1)

    record = FlightRecord(
        origin_iata=payload.origin_iata,
        destination_iata=payload.destination_iata,
        origin_city=payload.origin_city or o["city"],
        destination_city=payload.destination_city or d["city"],
        origin_country=payload.origin_country or o["country"],
        destination_country=payload.destination_country or d["country"],
        departed_at=departed.strftime("%Y-%m-%dT%H:%M:%SZ"),
        arrived_at=arrived.strftime("%Y-%m-%dT%H:%M:%SZ"),
        duration_mins=duration_mins,
        distance_km=distance_km,
        airline=payload.airline,
        flight_number=payload.flight_number,
        seat_class=payload.seat_class,
        notes=payload.notes,
        source="manual",
    )

    inserted = flights_table.insert(record)
    return {
        "status": "success",
        "inserted": inserted,
        "origin": {"iata": payload.origin_iata, "city": o["city"], "country": o["country"]},
        "destination": {"iata": payload.destination_iata, "city": d["city"], "country": d["country"]},
        "duration_mins": duration_mins,
        "distance_km": distance_km,
    }