"""
test_connection.py — Unit tests for database/connection.py pure helper.

Covers to_iso_str():
  - Unix int timestamp → UTC string with Z suffix
  - Unix float timestamp → UTC string with Z suffix
  - Naive datetime → treated as UTC
  - Aware UTC datetime → same string (no offset change)
  - Aware non-UTC datetime → converted to UTC
  - ISO 8601 string with offset → parsed and normalised to UTC
  - Output format is always "YYYY-MM-DDTHH:MM:SSZ"
"""

import pytest
from datetime import datetime, timezone, timedelta

from database.connection import to_iso_str


ISO_PATTERN = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$"


class TestToIsoStr:

    def test_output_always_ends_with_z(self):
        result = to_iso_str(0)
        assert result.endswith("Z")

    def test_output_format(self):
        import re
        result = to_iso_str(0)
        assert re.match(ISO_PATTERN, result), f"Unexpected format: {result}"

    def test_unix_int_epoch(self):
        result = to_iso_str(0)
        assert result == "1970-01-01T00:00:00Z"

    def test_unix_int_known_timestamp(self):
        # 2024-06-15 09:00:00 UTC = 1718442000
        result = to_iso_str(1718442000)
        assert result == "2024-06-15T09:00:00Z"

    def test_unix_float_truncates_subseconds(self):
        result = to_iso_str(1718442000.9)
        assert result == "2024-06-15T09:00:00Z"

    def test_naive_datetime_treated_as_utc(self):
        dt = datetime(2024, 6, 15, 9, 0, 0)  # no tzinfo
        result = to_iso_str(dt)
        assert result == "2024-06-15T09:00:00Z"

    def test_aware_utc_datetime(self):
        dt = datetime(2024, 6, 15, 9, 0, 0, tzinfo=timezone.utc)
        result = to_iso_str(dt)
        assert result == "2024-06-15T09:00:00Z"

    def test_aware_non_utc_datetime_converted(self):
        # +05:30 offset → UTC is 09:00 - 05:30 = 03:30
        tz_ist = timezone(timedelta(hours=5, minutes=30))
        dt = datetime(2024, 6, 15, 9, 0, 0, tzinfo=tz_ist)
        result = to_iso_str(dt)
        assert result == "2024-06-15T03:30:00Z"

    def test_iso_string_with_utc_offset(self):
        result = to_iso_str("2024-06-15T09:00:00+00:00")
        assert result == "2024-06-15T09:00:00Z"

    def test_iso_string_with_positive_offset_converted(self):
        # +01:00 → subtract 1 hour
        result = to_iso_str("2024-06-15T10:00:00+01:00")
        assert result == "2024-06-15T09:00:00Z"

    def test_iso_string_with_negative_offset_converted(self):
        # -05:00 → add 5 hours
        result = to_iso_str("2024-06-15T04:00:00-05:00")
        assert result == "2024-06-15T09:00:00Z"
