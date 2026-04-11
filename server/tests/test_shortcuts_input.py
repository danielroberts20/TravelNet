"""
test_shortcuts_input.py — Unit tests for upload/location/shortcuts.py input_csv().

Covers:
  - Missing required column raises HTTPException(400)
  - All three required columns checked (latitude, longitude, timestamp)
  - Valid rows are inserted and counted
  - Rows that fail validation are skipped, not inserted
  - send_notification called with inserted/skipped counts
  - Returns (inserted, skipped_rows) tuple
"""

import io
import pytest
from unittest.mock import patch, MagicMock

from fastapi import HTTPException

from upload.location.shortcuts import input_csv


def _make_csv(*rows, header=None):
    """Build a file-like CSV object from header + rows."""
    if header is None:
        header = ["latitude", "longitude", "timestamp", "speed"]
    lines = [",".join(header)]
    for row in rows:
        lines.append(",".join(str(v) for v in row))
    return io.StringIO("\n".join(lines))


def _run(csv_file):
    """Run input_csv with insert_log and send_notification mocked out."""
    with patch("upload.location.shortcuts.insert_log") as mock_insert, \
         patch("upload.location.shortcuts.send_notification") as mock_notif, \
         patch("upload.location.shortcuts.Log") as mock_log:
        # make Log.from_strings return a dummy object
        mock_log.from_strings.return_value = MagicMock()
        result = input_csv(csv_file)
    return result, mock_insert, mock_notif, mock_log


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestInputCsv:

    def test_missing_latitude_raises_400(self):
        csv_file = _make_csv(
            ["48.8566", "2024-06-15T08:00:00Z"],
            header=["longitude", "timestamp"],
        )
        with pytest.raises(HTTPException) as exc_info:
            input_csv(csv_file)
        assert exc_info.value.status_code == 400

    def test_missing_longitude_raises_400(self):
        csv_file = _make_csv(
            ["51.5074", "2024-06-15T08:00:00Z"],
            header=["latitude", "timestamp"],
        )
        with pytest.raises(HTTPException) as exc_info:
            input_csv(csv_file)
        assert exc_info.value.status_code == 400

    def test_missing_timestamp_raises_400(self):
        csv_file = _make_csv(
            ["51.5074", "-0.1278"],
            header=["latitude", "longitude"],
        )
        with pytest.raises(HTTPException) as exc_info:
            input_csv(csv_file)
        assert exc_info.value.status_code == 400

    def test_valid_rows_inserted(self):
        csv_file = _make_csv(
            ["51.5074", "-0.1278", "2024-06-15T08:00:00Z", "5.0"],
            ["48.8566", "2.3522",  "2024-06-15T09:00:00Z", "0.0"],
        )
        result, mock_insert, _, _ = _run(csv_file)
        inserted, skipped = result
        assert inserted == 2
        assert skipped == []
        assert mock_insert.call_count == 2

    def test_empty_csv_inserts_nothing(self):
        csv_file = _make_csv()  # header only, no rows
        result, mock_insert, _, _ = _run(csv_file)
        inserted, skipped = result
        assert inserted == 0
        assert skipped == []
        mock_insert.assert_not_called()

    def test_bad_row_is_skipped(self):
        csv_file = _make_csv(
            ["51.5074", "-0.1278", "2024-06-15T08:00:00Z", "5.0"],
        )
        with patch("upload.location.shortcuts.insert_log") as mock_insert, \
             patch("upload.location.shortcuts.send_notification"), \
             patch("upload.location.shortcuts.Log") as mock_log:
            # First call raises, simulating bad row
            mock_log.from_strings.side_effect = ValueError("bad data")
            result = input_csv(csv_file)
        inserted, skipped = result
        assert inserted == 0
        assert len(skipped) == 1
        mock_insert.assert_not_called()

    def test_mixed_good_and_bad_rows(self):
        csv_file = _make_csv(
            ["51.5074", "-0.1278", "2024-06-15T08:00:00Z", "5.0"],
            ["BAD",     "DATA",    "INVALID",               ""],
            ["48.8566", "2.3522",  "2024-06-15T10:00:00Z", "0.0"],
        )
        good_row = MagicMock()
        with patch("upload.location.shortcuts.insert_log") as mock_insert, \
             patch("upload.location.shortcuts.send_notification"), \
             patch("upload.location.shortcuts.Log") as mock_log:
            mock_log.from_strings.side_effect = [good_row, ValueError("bad"), good_row]
            result = input_csv(csv_file)
        inserted, skipped = result
        assert inserted == 2
        assert len(skipped) == 1

    def test_send_notification_called_once(self):
        csv_file = _make_csv(
            ["51.5074", "-0.1278", "2024-06-15T08:00:00Z", "5.0"],
        )
        _, _, mock_notif, _ = _run(csv_file)
        mock_notif.assert_called_once()

    def test_send_notification_receives_title(self):
        csv_file = _make_csv(
            ["51.5074", "-0.1278", "2024-06-15T08:00:00Z", "5.0"],
        )
        _, _, mock_notif, _ = _run(csv_file)
        call_kwargs = mock_notif.call_args
        args, kwargs = call_kwargs
        all_args = list(args) + list(kwargs.values())
        assert any("Shortcut" in str(a) for a in all_args) or kwargs.get("title") == "Shortcut Location"

    def test_returns_tuple(self):
        csv_file = _make_csv()
        result, _, _, _ = _run(csv_file)
        assert isinstance(result, tuple)
        assert len(result) == 2
