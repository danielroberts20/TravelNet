from dataclasses import dataclass
import json

from parsers import parse_float, parse_bool_yes_no, parse_int

@dataclass
class CellularState:
    provider_name: str
    radio: str
    code: str
    is_roaming: bool

    @classmethod
    def from_json(self, **kwargs):
        return self(
            provider_name = kwargs["provider_name"],
            radio = kwargs["radio"],
            code = kwargs["code"],
            is_roaming = bool(kwargs["is_roaming"])
        )

@dataclass
class Log:
    timestamp: int
    timezone: str | None
    country_code: str | None
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
        return self(
            timestamp=parse_int(kwargs["timestamp"]),
            timezone=kwargs["timezone"],
            country_code=kwargs["country_code"],
            latitude=parse_float(kwargs["latitude"]),
            longitude=parse_float(kwargs["longitude"]),
            altitude=parse_float(kwargs["altitude"]),
            activity=kwargs["activity"],
            device=kwargs["device"],
            is_locked=kwargs["is_locked"].lower() == "yes",
            battery=parse_int(kwargs["battery"]),
            is_charging=kwargs["charging"].lower() == "yes",
            is_connected_charger=parse_bool_yes_no(kwargs["connected_charger"]),
            BSSID=kwargs["BSSID"],
            RSSI=parse_int(kwargs["RSSI"]),
            cellular_states=[
                CellularState.from_json(**i)
                for i in json.loads(kwargs["cellular_states"])
                ]
        )
