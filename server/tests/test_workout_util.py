"""
test_workout_util.py — Unit tests for upload/health/workout_util.py pure helpers.

Covers:
  - parse_unix: HAE workout datetime format
  - _qty: safe extraction of 'qty' from HAE quantity dicts
  - _units: safe extraction of 'units' from HAE quantity dicts
"""

import pytest

from upload.health.workouts import _qty, _units
from upload.health.processing import parse_unix


# ---------------------------------------------------------------------------
# parse_unix
# ---------------------------------------------------------------------------

class TestParseUnix:

    def test_standard_hae_format(self):
        ts = parse_unix("2024-06-15 09:30:00 +0100")
        assert isinstance(ts, int)
        assert ts > 1_700_000_000

    def test_negative_utc_offset(self):
        ts_plus = parse_unix("2024-06-15 09:30:00 +0000")
        ts_minus = parse_unix("2024-06-15 09:30:00 -0500")
        # -0500 is 5 hours behind UTC, so the UTC equivalent is later
        assert ts_minus > ts_plus

    def test_strips_whitespace(self):
        ts = parse_unix("  2024-06-15 09:30:00 +0000  ")
        assert ts > 0

    def test_invalid_format_raises(self):
        with pytest.raises(ValueError):
            parse_unix("2024/06/15")  # slashes — wrong separator for strptime

    def test_invalid_date_raises(self):
        with pytest.raises(ValueError):
            parse_unix("not-a-date +0000")


# ---------------------------------------------------------------------------
# _qty
# ---------------------------------------------------------------------------

class TestQty:

    def test_returns_qty_value(self):
        assert _qty({"qty": 42.5, "units": "km"}) == pytest.approx(42.5)

    def test_returns_none_when_obj_is_none(self):
        assert _qty(None) is None

    def test_returns_none_when_qty_key_missing(self):
        assert _qty({"units": "km"}) is None

    def test_returns_none_qty_value(self):
        assert _qty({"qty": None}) is None

    def test_returns_zero(self):
        assert _qty({"qty": 0}) == pytest.approx(0.0)

    def test_returns_negative(self):
        assert _qty({"qty": -10.0}) == pytest.approx(-10.0)


# ---------------------------------------------------------------------------
# _units
# ---------------------------------------------------------------------------

class TestUnits:

    def test_returns_units_value(self):
        assert _units({"qty": 42.5, "units": "km"}) == "km"

    def test_returns_none_when_obj_is_none(self):
        assert _units(None) is None

    def test_returns_none_when_units_key_missing(self):
        assert _units({"qty": 10}) is None

    def test_returns_units_string(self):
        assert _units({"units": "kcal"}) == "kcal"
