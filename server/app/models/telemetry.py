"""
telemetry_models.py
~~~~~~~~~~~~~~~~~~~
Pydantic and dataclass models for all telemetry data ingested by TravelNet.

Two groups:
  - CellularState / Log   — Shortcuts iOS automation CSV upload (legacy path)
  - Overland*             — Overland GPS app GeoJSON upload (modern path)
"""

from dataclasses import dataclass
from typing import Optional

from models.parsers import parse_float, parse_bool_yes_no, parse_int, parse_string, parse_cellular_states
from pydantic import BaseModel, Field, field_validator


@dataclass
class CellularState:
    """A single cellular network state snapshot attached to a location fix."""

    provider_name: str
    radio: str
    code: str
    is_roaming: bool

    @classmethod
    def from_json(self, **kwargs):
        """Construct a CellularState from a parsed JSON dict."""
        return self(
            provider_name = kwargs["provider_name"],
            radio = kwargs["radio"],
            code = kwargs["code"],
            is_roaming = bool(kwargs["is_roaming"])
        )


@dataclass
class Log:
    """A single row from a Shortcuts location CSV export.

    Includes the device's GPS fix, battery/charging state, Wi-Fi BSSID/RSSI,
    and a list of concurrent cellular network states.
    """

    timestamp: int
    latitude: float
    longitude: float
    altitude: float | None
    activity: str | None
    device: str | None
    is_locked: bool | None
    battery: int | None
    is_charging: bool | None
    is_connected_charger: bool | None
    BSSID: str | None
    RSSI: int | None
    cellular_states: list[CellularState] | None

    @classmethod
    def from_strings(self, **kwargs):
        """Construct a Log from raw CSV string values (all fields are strings)."""
        return self(
            timestamp=parse_int(kwargs["timestamp"]),
            latitude=parse_float(kwargs["latitude"]),
            longitude=parse_float(kwargs["longitude"]),
            altitude=parse_float(kwargs["altitude"]),
            activity=parse_string(kwargs["activity"]),
            device=parse_string(kwargs["device"]),
            is_locked=parse_bool_yes_no(kwargs["is_locked"]),
            battery=parse_int(kwargs["battery"]),
            is_charging=parse_bool_yes_no(kwargs["charging"]),
            is_connected_charger=parse_bool_yes_no(kwargs["connected_charger"]),
            BSSID=parse_string(kwargs["BSSID"]),
            RSSI=parse_int(kwargs["RSSI"]),
            cellular_states=parse_cellular_states(kwargs["cellular_states"])
        )


# ---------------------------------------------------------------------------
# Pydantic models — mirrors the Overland GeoJSON schema exactly
# ---------------------------------------------------------------------------

class OverlandGeometry(BaseModel):
    """GeoJSON geometry object as sent by the Overland iOS app."""

    type: str
    coordinates: list[float]          # [lon, lat] — note GeoJSON order


class OverlandProperties(BaseModel):
    """Properties block of an Overland GeoJSON Feature."""

    timestamp: str                    # ISO 8601 with TZ offset
    altitude: Optional[float] = None
    speed: Optional[float] = None     # m/s, -1 if unavailable
    horizontal_accuracy: Optional[float] = None   # metres
    vertical_accuracy: Optional[float] = None     # metres, -1 if unavailable
    motion: Optional[list[str]] = Field(default_factory=list)
    pauses: Optional[bool] = None
    activity: Optional[str] = None
    desired_accuracy: Optional[float] = None
    deferred: Optional[float] = None
    significant_change: Optional[str] = None
    locations_in_payload: Optional[int] = None
    device_id: Optional[str] = None
    wifi: Optional[str] = None        # SSID of connected network
    battery_state: Optional[str] = None   # "charging" | "full" | "unplugged"
    battery_level: Optional[float] = None  # 0.0–1.0

    @field_validator("speed", "vertical_accuracy", mode="before")
    @classmethod
    def negative_to_none(cls, v):
        """Overland uses -1 to signal 'unavailable'; normalise to NULL."""
        if v is not None and float(v) < 0:
            return None
        return v


class OverlandFeature(BaseModel):
    """A single GeoJSON Feature as sent inside an Overland payload."""

    type: str
    geometry: OverlandGeometry
    properties: OverlandProperties


class OverlandPayload(BaseModel):
    """Top-level object POSTed by the Overland iOS app.

    Contains a list of Feature objects, one per location fix in the batch.
    """

    locations: list[OverlandFeature]
