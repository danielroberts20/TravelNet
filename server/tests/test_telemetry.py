"""
test_telemetry.py — Unit tests for models/telemetry.py.

Covers:
  - CellularState.from_json: construction, is_roaming bool coercion
  - Log.from_strings: full construction from CSV string kwargs
  - OverlandProperties.negative_to_none: speed and vertical_accuracy only
  - horizontal_accuracy: NOT subject to negative_to_none (stays as-is)
  - OverlandPayload: round-trip construction from GeoJSON dict
"""

import json
import pytest
from pydantic import ValidationError

from models.telemetry import (
    CellularState,
    Log,
    OverlandProperties,
    OverlandFeature,
    OverlandGeometry,
    OverlandPayload,
)


# ---------------------------------------------------------------------------
# CellularState.from_json
# ---------------------------------------------------------------------------

class TestCellularStateFromJson:

    def _make(self, **overrides):
        base = {
            "provider_name": "EE",
            "radio": "LTE",
            "code": "234-10",
            "is_roaming": False,
        }
        base.update(overrides)
        return CellularState.from_json(**base)

    def test_basic_construction(self):
        s = self._make()
        assert s.provider_name == "EE"
        assert s.radio == "LTE"
        assert s.code == "234-10"
        assert s.is_roaming is False

    def test_is_roaming_int_zero_is_false(self):
        s = self._make(is_roaming=0)
        assert s.is_roaming is False

    def test_is_roaming_int_one_is_true(self):
        s = self._make(is_roaming=1)
        assert s.is_roaming is True

    def test_is_roaming_bool_true(self):
        s = self._make(is_roaming=True)
        assert s.is_roaming is True

    def test_is_roaming_bool_false(self):
        s = self._make(is_roaming=False)
        assert s.is_roaming is False

    def test_is_roaming_nonempty_string_is_true(self):
        # bool("false") → True — non-empty string is truthy
        s = self._make(is_roaming="false")
        assert s.is_roaming is True

    def test_missing_key_raises(self):
        with pytest.raises(KeyError):
            CellularState.from_json(radio="LTE", code="234", is_roaming=False)


# ---------------------------------------------------------------------------
# Log.from_strings
# ---------------------------------------------------------------------------

class TestLogFromStrings:

    def _base_kwargs(self):
        return {
            "timestamp": "1718444400",
            "latitude": "51.5074",
            "longitude": "-0.1278",
            "altitude": "15.3",
            "device": "iPhone",
            "is_locked": "Yes",
            "battery": "85",
            "charging": "No",
            "connected_charger": "No",
            "BSSID": "AA:BB:CC:DD:EE:FF",
            "RSSI": "-65",
            "cellular_states": None,
        }

    def test_full_construction(self):
        log = Log.from_strings(**self._base_kwargs())
        assert log.timestamp == 1718444400
        assert log.latitude == pytest.approx(51.5074)
        assert log.longitude == pytest.approx(-0.1278)
        assert log.altitude == pytest.approx(15.3)
        assert log.device == "iPhone"
        assert log.is_locked is True
        assert log.battery == 85
        assert log.is_charging is False
        assert log.is_connected_charger is False
        assert log.BSSID == "AA:BB:CC:DD:EE:FF"
        assert log.RSSI == -65
        assert log.cellular_states is None

    def test_empty_optional_fields_become_none(self):
        kwargs = self._base_kwargs()
        kwargs["altitude"] = ""
        kwargs["device"] = ""
        kwargs["BSSID"] = ""
        log = Log.from_strings(**kwargs)
        assert log.altitude is None
        assert log.device is None
        assert log.BSSID is None

    def test_cellular_states_parsed_from_json(self):
        kwargs = self._base_kwargs()
        kwargs["cellular_states"] = json.dumps([
            {"provider_name": "EE", "radio": "LTE", "code": "234", "is_roaming": False}
        ])
        log = Log.from_strings(**kwargs)
        assert log.cellular_states is not None
        assert len(log.cellular_states) == 1
        assert log.cellular_states[0].provider_name == "EE"


# ---------------------------------------------------------------------------
# OverlandProperties — negative_to_none validator
# ---------------------------------------------------------------------------

class TestOverlandPropertiesNegativeToNone:

    def _make(self, **overrides):
        base = {"timestamp": "2024-06-15T09:00:00+00:00"}
        base.update(overrides)
        return OverlandProperties(**base)

    def test_speed_negative_one_becomes_none(self):
        p = self._make(speed=-1)
        assert p.speed is None

    def test_speed_negative_becomes_none(self):
        p = self._make(speed=-0.5)
        assert p.speed is None

    def test_speed_zero_kept(self):
        p = self._make(speed=0.0)
        assert p.speed == pytest.approx(0.0)

    def test_speed_positive_kept(self):
        p = self._make(speed=5.2)
        assert p.speed == pytest.approx(5.2)

    def test_speed_none_kept_as_none(self):
        p = self._make(speed=None)
        assert p.speed is None

    def test_vertical_accuracy_negative_one_becomes_none(self):
        p = self._make(vertical_accuracy=-1)
        assert p.vertical_accuracy is None

    def test_vertical_accuracy_positive_kept(self):
        p = self._make(vertical_accuracy=3.0)
        assert p.vertical_accuracy == pytest.approx(3.0)

    def test_horizontal_accuracy_negative_NOT_normalised(self):
        # horizontal_accuracy is explicitly excluded from negative_to_none
        p = self._make(horizontal_accuracy=-1.0)
        assert p.horizontal_accuracy == pytest.approx(-1.0)

    def test_horizontal_accuracy_positive_kept(self):
        p = self._make(horizontal_accuracy=10.0)
        assert p.horizontal_accuracy == pytest.approx(10.0)


# ---------------------------------------------------------------------------
# OverlandPayload — GeoJSON coordinate order
# ---------------------------------------------------------------------------

class TestOverlandPayload:

    def _make_payload(self, lon=2.3522, lat=48.8566):
        return {
            "locations": [
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [lon, lat],  # GeoJSON order: [lon, lat]
                    },
                    "properties": {
                        "timestamp": "2024-06-15T09:00:00+00:00",
                        "speed": 1.2,
                        "horizontal_accuracy": 8.0,
                    },
                }
            ]
        }

    def test_parse_single_feature(self):
        payload = OverlandPayload(**self._make_payload())
        assert len(payload.locations) == 1

    def test_coordinate_order_lon_then_lat(self):
        lon, lat = 2.3522, 48.8566
        payload = OverlandPayload(**self._make_payload(lon=lon, lat=lat))
        coords = payload.locations[0].geometry.coordinates
        assert coords[0] == pytest.approx(lon)   # index 0 = longitude
        assert coords[1] == pytest.approx(lat)   # index 1 = latitude

    def test_empty_locations_list(self):
        payload = OverlandPayload(locations=[])
        assert payload.locations == []
