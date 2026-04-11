"""
test_overland_util.py — Unit tests for database/location/overland/util.py.

Covers _normalise_ts():
  - UTC ISO string → "YYYY-MM-DD HH:MM:SS" (SQLite-sortable, no timezone info)
  - Offset-aware string → converted to UTC, then formatted
  - Naive string → treated as UTC
  - Unparseable string → returned as-is (don't silently drop the row)
"""

import pytest

from database.location.overland.util import _normalise_ts


class TestNormaliseTs:

    def test_utc_z_suffix(self):
        result = _normalise_ts("2024-06-15T09:30:00+00:00")
        assert result == "2024-06-15 09:30:00"

    def test_output_has_no_timezone_info(self):
        result = _normalise_ts("2024-06-15T09:30:00+00:00")
        assert "Z" not in result
        assert "+" not in result
        assert "-" not in result.split(" ")[0][8:]  # no tz sign after date

    def test_output_format_is_sqlite_sortable(self):
        # "YYYY-MM-DD HH:MM:SS" — space separator, no T, no Z
        result = _normalise_ts("2024-06-15T09:30:00+00:00")
        parts = result.split(" ")
        assert len(parts) == 2
        assert len(parts[0]) == 10  # YYYY-MM-DD
        assert len(parts[1]) == 8   # HH:MM:SS

    def test_positive_offset_converted_to_utc(self):
        # +05:30 IST — subtract 5h30m to get UTC
        result = _normalise_ts("2024-06-15T14:30:00+05:30")
        assert result == "2024-06-15 09:00:00"

    def test_negative_offset_converted_to_utc(self):
        # -05:00 EST — add 5h to get UTC
        result = _normalise_ts("2024-06-15T04:00:00-05:00")
        assert result == "2024-06-15 09:00:00"

    def test_naive_string_treated_as_utc(self):
        result = _normalise_ts("2024-06-15T09:30:00")
        assert result == "2024-06-15 09:30:00"

    def test_unparseable_returned_as_is(self):
        garbage = "not-a-timestamp"
        result = _normalise_ts(garbage)
        assert result == garbage

    def test_partially_valid_unparseable_returned_as_is(self):
        bad = "2024-99-99T00:00:00"
        result = _normalise_ts(bad)
        assert result == bad
