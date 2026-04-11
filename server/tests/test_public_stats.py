"""
test_public_stats.py — Unit tests for public/stats.py pure query helpers.

Covers get_trip_status():
  - meta.status_override in valid set → returned directly (pre_departure, travelling, finished)
  - invalid override falls through to date inference
  - no legs → pre_departure
  - all legs have actual.departure → finished
  - at least one leg arrival in the past → travelling
  - all leg arrivals in the future → pre_departure

Covers get_current_leg():
  - meta.current_leg_override with matching id → that leg returned
  - override id not found → falls through to date inference
  - leg arrival passed, no departure yet → current leg
  - leg arrival passed, departure in future → current leg
  - leg arrival passed, departure also passed → not current (moved on)
  - leg arrival in future → not yet current
  - no legs → None

Covers get_countries_visited():
  - no legs → {full:0, stopover:0, total:0}
  - leg with future arrival not counted
  - non-stopover leg with past arrival → full+1
  - stopover leg with past arrival → stopover+1
  - total = full + stopover
"""

from datetime import date
import pytest
from unittest.mock import patch

from public.stats import get_trip_status, get_current_leg, get_countries_visited


# ---------------------------------------------------------------------------
# Date patch helper — subclass keeps fromisoformat / isinstance working
# ---------------------------------------------------------------------------

class _FakeDate(date):
    """date subclass with a controllable today()."""
    _today: date = date(2026, 4, 11)

    @classmethod
    def today(cls):
        return cls._today


def _patch_today(today: date):
    """Context manager: patch public.stats.date so today() returns `today`."""
    _FakeDate._today = today
    return patch("public.stats.date", _FakeDate)


# ---------------------------------------------------------------------------
# Helpers for building leg dicts
# ---------------------------------------------------------------------------

def _leg(leg_id, arrival, departure=None, stopover=False, actual_arrival=None, actual_departure=None):
    leg = {"id": leg_id, "name": leg_id, "stopover": stopover}
    planned = {}
    if arrival:
        planned["arrival"] = str(arrival)
    if departure:
        planned["departure"] = str(departure)
    leg["planned"] = planned

    actual = {}
    if actual_arrival:
        actual["arrival"] = str(actual_arrival)
    if actual_departure:
        actual["departure"] = str(actual_departure)
    leg["actual"] = actual

    return leg


# ---------------------------------------------------------------------------
# get_trip_status
# ---------------------------------------------------------------------------

class TestGetTripStatus:

    def test_override_pre_departure(self):
        data = {"meta": {"status_override": "pre_departure"}, "legs": []}
        assert get_trip_status(data) == "pre_departure"

    def test_override_travelling(self):
        data = {"meta": {"status_override": "travelling"}, "legs": []}
        assert get_trip_status(data) == "travelling"

    def test_override_finished(self):
        data = {"meta": {"status_override": "finished"}, "legs": []}
        assert get_trip_status(data) == "finished"

    def test_invalid_override_falls_through(self):
        # "unknown" is not a valid override value → date inference runs
        data = {"meta": {"status_override": "unknown"}, "legs": []}
        with _patch_today(date(2026, 4, 11)):
            result = get_trip_status(data)
        # No legs → pre_departure
        assert result == "pre_departure"

    def test_no_legs_returns_pre_departure(self):
        data = {"legs": []}
        with _patch_today(date(2026, 4, 11)):
            result = get_trip_status(data)
        assert result == "pre_departure"

    def test_no_legs_key_returns_pre_departure(self):
        data = {}
        with _patch_today(date(2026, 4, 11)):
            result = get_trip_status(data)
        assert result == "pre_departure"

    def test_all_legs_have_actual_departure_returns_finished(self):
        data = {"legs": [
            _leg("leg1", "2025-06-01", actual_departure="2025-09-01"),
            _leg("leg2", "2025-09-01", actual_departure="2025-12-01"),
        ]}
        with _patch_today(date(2026, 4, 11)):
            result = get_trip_status(data)
        assert result == "finished"

    def test_one_leg_missing_actual_departure_not_finished(self):
        data = {"legs": [
            _leg("leg1", "2025-06-01", actual_departure="2025-09-01"),
            _leg("leg2", "2025-09-01"),  # no actual departure
        ]}
        with _patch_today(date(2026, 4, 11)):
            result = get_trip_status(data)
        assert result != "finished"

    def test_past_arrival_returns_travelling(self):
        today = date(2026, 4, 11)
        data = {"legs": [_leg("leg1", "2025-06-01")]}  # arrival in the past
        with _patch_today(today):
            result = get_trip_status(data)
        assert result == "travelling"

    def test_all_future_arrivals_returns_pre_departure(self):
        today = date(2026, 4, 11)
        data = {"legs": [_leg("leg1", "2027-01-01")]}  # arrival in the future
        with _patch_today(today):
            result = get_trip_status(data)
        assert result == "pre_departure"

    def test_arrival_today_returns_travelling(self):
        today = date(2026, 4, 11)
        data = {"legs": [_leg("leg1", str(today))]}
        with _patch_today(today):
            result = get_trip_status(data)
        assert result == "travelling"


# ---------------------------------------------------------------------------
# get_current_leg
# ---------------------------------------------------------------------------

class TestGetCurrentLeg:

    def test_override_id_returns_matching_leg(self):
        leg_a = _leg("leg_a", "2025-06-01")
        leg_b = _leg("leg_b", "2026-01-01")
        data = {"meta": {"current_leg_override": "leg_b"}, "legs": [leg_a, leg_b]}
        with _patch_today(date(2025, 7, 1)):  # before leg_b's arrival
            result = get_current_leg(data)
        assert result["id"] == "leg_b"

    def test_override_id_not_found_falls_through(self):
        leg_a = _leg("leg_a", "2025-06-01")  # arrival in the past
        data = {"meta": {"current_leg_override": "nonexistent"}, "legs": [leg_a]}
        with _patch_today(date(2026, 4, 11)):
            result = get_current_leg(data)
        # Falls through to date inference → leg_a is current
        assert result is not None
        assert result["id"] == "leg_a"

    def test_leg_with_past_arrival_no_departure_is_current(self):
        leg = _leg("leg1", "2025-06-01")
        data = {"legs": [leg]}
        with _patch_today(date(2026, 4, 11)):
            result = get_current_leg(data)
        assert result is not None
        assert result["id"] == "leg1"

    def test_leg_with_past_arrival_and_future_departure_is_current(self):
        leg = _leg("leg1", "2025-06-01", departure="2027-01-01")
        data = {"legs": [leg]}
        with _patch_today(date(2026, 4, 11)):
            result = get_current_leg(data)
        assert result is not None
        assert result["id"] == "leg1"

    def test_leg_with_past_arrival_and_past_departure_not_current(self):
        leg = _leg("leg1", "2025-06-01", departure="2025-09-01")
        data = {"legs": [leg]}
        with _patch_today(date(2026, 4, 11)):
            result = get_current_leg(data)
        assert result is None

    def test_leg_with_future_arrival_not_current(self):
        leg = _leg("leg1", "2027-01-01")
        data = {"legs": [leg]}
        with _patch_today(date(2026, 4, 11)):
            result = get_current_leg(data)
        assert result is None

    def test_no_legs_returns_none(self):
        data = {"legs": []}
        with _patch_today(date(2026, 4, 11)):
            result = get_current_leg(data)
        assert result is None

    def test_returns_first_active_leg(self):
        """When multiple legs are active, returns the first one."""
        leg_a = _leg("leg_a", "2025-06-01")
        leg_b = _leg("leg_b", "2025-08-01")
        data = {"legs": [leg_a, leg_b]}
        with _patch_today(date(2026, 4, 11)):
            result = get_current_leg(data)
        assert result["id"] == "leg_a"

    def test_actual_arrival_takes_priority_over_planned(self):
        """actual.arrival overrides planned.arrival for inference."""
        leg = {
            "id": "leg1",
            "planned": {"arrival": "2027-01-01"},  # future
            "actual": {"arrival": "2025-06-01"},    # past
        }
        data = {"legs": [leg]}
        with _patch_today(date(2026, 4, 11)):
            result = get_current_leg(data)
        assert result is not None
        assert result["id"] == "leg1"


# ---------------------------------------------------------------------------
# get_countries_visited
# ---------------------------------------------------------------------------

class TestGetCountriesVisited:

    def test_no_legs_returns_zeros(self):
        data = {"legs": []}
        with _patch_today(date(2026, 4, 11)):
            result = get_countries_visited(data)
        assert result == {"full": 0, "stopover": 0, "total": 0}

    def test_future_arrival_not_counted(self):
        data = {"legs": [_leg("leg1", "2027-01-01")]}
        with _patch_today(date(2026, 4, 11)):
            result = get_countries_visited(data)
        assert result["total"] == 0

    def test_past_arrival_non_stopover_counts_as_full(self):
        data = {"legs": [_leg("leg1", "2025-06-01", stopover=False)]}
        with _patch_today(date(2026, 4, 11)):
            result = get_countries_visited(data)
        assert result["full"] == 1
        assert result["stopover"] == 0

    def test_past_arrival_stopover_counts_as_stopover(self):
        data = {"legs": [_leg("leg1", "2025-06-01", stopover=True)]}
        with _patch_today(date(2026, 4, 11)):
            result = get_countries_visited(data)
        assert result["stopover"] == 1
        assert result["full"] == 0

    def test_total_equals_full_plus_stopover(self):
        data = {"legs": [
            _leg("leg1", "2025-06-01", stopover=False),
            _leg("leg2", "2025-08-01", stopover=True),
            _leg("leg3", "2025-10-01", stopover=False),
        ]}
        with _patch_today(date(2026, 4, 11)):
            result = get_countries_visited(data)
        assert result["full"] == 2
        assert result["stopover"] == 1
        assert result["total"] == 3

    def test_mix_of_past_and_future_arrivals(self):
        data = {"legs": [
            _leg("leg1", "2025-06-01"),   # past → counted
            _leg("leg2", "2027-01-01"),   # future → not counted
        ]}
        with _patch_today(date(2026, 4, 11)):
            result = get_countries_visited(data)
        assert result["total"] == 1

    def test_arrival_today_is_counted(self):
        today = date(2026, 4, 11)
        data = {"legs": [_leg("leg1", str(today))]}
        with _patch_today(today):
            result = get_countries_visited(data)
        assert result["total"] == 1
