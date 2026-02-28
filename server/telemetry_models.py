from dataclasses import dataclass

from parsers import parse_float, parse_bool_yes_no, parse_int, parse_string, parse_cellular_states

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
            timezone=parse_string(kwargs["timezone"]),
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
