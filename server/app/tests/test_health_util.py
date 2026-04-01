"""
test_health_util.py — Unit tests for upload/health/health_util.py pure helpers.

Covers:
  - parse_unix: datetime and date-only strings
  - bucket_timestamp: snaps timestamps to interval boundaries
  - _parse_sources: pipe-delimited source parsing
  - _get_agg_type: aggregation type lookup with fallbacks
  - _aggregate: sum / min / max / mean dispatch
"""

import pytest

from upload.health.processing import (
    _aggregate,
    _get_agg_type,
    _parse_sources,
    bucket_timestamp,
    parse_unix,
)


# ---------------------------------------------------------------------------
# parse_unix
# ---------------------------------------------------------------------------

class TestParseUnix:

    def test_datetime_string_with_timezone(self):
        # "2024-02-06 14:30:00 -0800" → UTC+8h30 = 2024-02-06 22:30:00 UTC
        ts = parse_unix("2024-02-06 14:30:00 -0800")
        assert isinstance(ts, int)
        # Rough sanity: value is in a reasonable Unix range
        assert ts > 1_700_000_000

    def test_date_only_string(self):
        # "2024-02-06" treated as midnight local time
        ts = parse_unix("2024-02-06")
        assert isinstance(ts, int)
        assert ts > 0

    def test_date_only_smaller_than_datetime(self):
        # Midnight must be <= any time on the same day
        date_ts = parse_unix("2024-02-06")
        dt_ts = parse_unix("2024-02-06 12:00:00 +0000")
        assert date_ts <= dt_ts

    def test_invalid_format_raises(self):
        with pytest.raises(ValueError):
            parse_unix("06-02-2024")

    def test_strips_whitespace(self):
        ts = parse_unix("  2024-02-06  ")
        assert ts > 0


# ---------------------------------------------------------------------------
# bucket_timestamp
# ---------------------------------------------------------------------------

class TestBucketTimestamp:

    def test_already_on_boundary(self):
        # 10:00:00 UTC on some day — bucket at 5-min interval should be unchanged
        # 10:00:00 = 36000 seconds past midnight
        # Pick a known Unix timestamp that is exactly at a 5-minute mark
        ts = 1_700_000_000  # arbitrary
        interval = 5
        interval_s = interval * 60
        # Round down to a clean multiple
        clean = (ts // interval_s) * interval_s
        assert bucket_timestamp(clean, interval) == clean

    def test_snaps_down_to_start_of_bucket(self):
        interval = 5
        interval_s = interval * 60
        base = 1_700_000_000
        # Add 2 minutes 30 seconds into a bucket
        ts = base + 150
        bucketed = bucket_timestamp(ts, interval)
        assert bucketed <= ts
        assert ts - bucketed < interval_s

    def test_one_minute_interval(self):
        # Find a clean 1-minute boundary then add 1 second
        boundary = (1_700_000_000 // 60) * 60  # 1699999980
        ts = boundary + 1
        bucketed = bucket_timestamp(ts, 1)
        assert bucketed == boundary

    def test_result_is_divisible_by_interval_seconds(self):
        interval = 15
        ts = 1_700_001_234
        bucketed = bucket_timestamp(ts, interval)
        assert bucketed % (interval * 60) == 0


# ---------------------------------------------------------------------------
# _parse_sources
# ---------------------------------------------------------------------------

class TestParseSources:

    def test_single_source(self):
        assert _parse_sources({"source": "iPhone"}) == ["iPhone"]

    def test_pipe_delimited_sources(self):
        result = _parse_sources({"source": "iPhone|Apple Watch"})
        assert result == ["iPhone", "Apple Watch"]

    def test_empty_source_returns_empty_list(self):
        assert _parse_sources({"source": ""}) == []

    def test_missing_source_key_returns_empty_list(self):
        assert _parse_sources({}) == []

    def test_strips_whitespace_around_each_source(self):
        result = _parse_sources({"source": " iPhone | Apple Watch "})
        assert result == ["iPhone", "Apple Watch"]


# ---------------------------------------------------------------------------
# _get_agg_type
# ---------------------------------------------------------------------------

class TestGetAggType:

    def test_known_metric_and_sub_metric(self):
        assert _get_agg_type("Step Count", "Step Count (count)") == "sum"

    def test_known_metric_unknown_sub_metric_returns_first_rule(self):
        # Step Count only has one rule; any sub_metric not in the rules dict
        # should fall back to the first (and only) rule value
        result = _get_agg_type("Step Count", "nonexistent_sub_metric")
        assert result == "sum"

    def test_unknown_metric_returns_mean(self):
        assert _get_agg_type("Unknown Metric", "anything") == "mean"

    def test_heart_rate_min_field(self):
        assert _get_agg_type("Heart Rate", "Min (count/min)") == "min"

    def test_heart_rate_avg_field(self):
        assert _get_agg_type("Heart Rate", "Avg (count/min)") == "mean"

    def test_heart_rate_max_field(self):
        assert _get_agg_type("Heart Rate", "Max (count/min)") == "max"

    def test_blood_oxygen_saturation_is_mean(self):
        assert _get_agg_type("Blood Oxygen Saturation", "Blood Oxygen Saturation (%)") == "mean"

    def test_active_energy_is_sum(self):
        assert _get_agg_type("Active Energy", "Active Energy (kJ)") == "sum"


# ---------------------------------------------------------------------------
# _aggregate
# ---------------------------------------------------------------------------

class TestAggregate:

    def test_sum(self):
        assert _aggregate([1.0, 2.0, 3.0], "sum") == pytest.approx(6.0)

    def test_min(self):
        assert _aggregate([3.0, 1.0, 2.0], "min") == pytest.approx(1.0)

    def test_max(self):
        assert _aggregate([3.0, 1.0, 2.0], "max") == pytest.approx(3.0)

    def test_mean(self):
        assert _aggregate([1.0, 2.0, 3.0], "mean") == pytest.approx(2.0)

    def test_unknown_agg_type_defaults_to_mean(self):
        assert _aggregate([10.0, 20.0], "median") == pytest.approx(15.0)

    def test_single_element(self):
        assert _aggregate([42.0], "sum") == pytest.approx(42.0)
        assert _aggregate([42.0], "mean") == pytest.approx(42.0)
        assert _aggregate([42.0], "min") == pytest.approx(42.0)
        assert _aggregate([42.0], "max") == pytest.approx(42.0)
