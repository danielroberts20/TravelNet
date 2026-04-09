"""
test_overland_backup.py — Unit tests for upload/location/overland/backup.py.

Covers:
  - append_to_daily_buffer: file creation, JSONL line format, daily rollover
  - log_previous_day_backup: no file → no-op, empty file → no-op, valid file → INFO log
"""

import json
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from models.telemetry import OverlandFeature, OverlandGeometry, OverlandPayload, OverlandProperties
from upload.location.overland.backup import append_to_daily_buffer, log_previous_day_backup


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_payload(n_locations: int = 2) -> OverlandPayload:
    """Build a minimal OverlandPayload with n_locations points."""
    features = []
    for i in range(n_locations):
        features.append(OverlandFeature(
            type="Feature",
            geometry=OverlandGeometry(type="Point", coordinates=[-0.1 + i * 0.01, 51.5 + i * 0.01]),
            properties=OverlandProperties(timestamp=f"2026-03-01T10:0{i}:00+00:00"),
        ))
    return OverlandPayload(locations=features)


# ---------------------------------------------------------------------------
# append_to_daily_buffer
# ---------------------------------------------------------------------------

class TestAppendToDailyBuffer:

    def test_creates_file_on_first_call(self, tmp_path):
        payload = _make_payload(1)
        with patch("upload.location.overland.backup.LOCATION_OVERLAND_BACKUP_DIR", tmp_path):
            append_to_daily_buffer(payload)

        files = list(tmp_path.glob("*.jsonl"))
        assert len(files) == 1

    def test_file_named_with_todays_date(self, tmp_path):
        payload = _make_payload(1)
        with patch("upload.location.overland.backup.LOCATION_OVERLAND_BACKUP_DIR", tmp_path), \
             patch("upload.location.overland.backup.datetime") as mock_dt:
            mock_dt.now.return_value.strftime.return_value = "2026-03-01"
            append_to_daily_buffer(payload)

        assert (tmp_path / "2026-03-01.jsonl").exists()

    def test_each_call_appends_one_line(self, tmp_path):
        payload = _make_payload(3)
        with patch("upload.location.overland.backup.LOCATION_OVERLAND_BACKUP_DIR", tmp_path), \
             patch("upload.location.overland.backup.datetime") as mock_dt:
            mock_dt.now.return_value.strftime.return_value = "2026-03-01"
            append_to_daily_buffer(payload)
            append_to_daily_buffer(payload)

        lines = (tmp_path / "2026-03-01.jsonl").read_text().splitlines()
        assert len(lines) == 2

    def test_line_is_valid_json(self, tmp_path):
        payload = _make_payload(2)
        with patch("upload.location.overland.backup.LOCATION_OVERLAND_BACKUP_DIR", tmp_path), \
             patch("upload.location.overland.backup.datetime") as mock_dt:
            mock_dt.now.return_value.strftime.return_value = "2026-03-01"
            append_to_daily_buffer(payload)

        line = (tmp_path / "2026-03-01.jsonl").read_text().strip()
        data = json.loads(line)
        assert "locations" in data
        assert len(data["locations"]) == 2

    def test_different_days_write_to_different_files(self, tmp_path):
        payload = _make_payload(1)
        with patch("upload.location.overland.backup.LOCATION_OVERLAND_BACKUP_DIR", tmp_path), \
             patch("upload.location.overland.backup.datetime") as mock_dt:
            mock_dt.now.return_value.strftime.return_value = "2026-03-01"
            append_to_daily_buffer(payload)
            mock_dt.now.return_value.strftime.return_value = "2026-03-02"
            append_to_daily_buffer(payload)

        assert (tmp_path / "2026-03-01.jsonl").exists()
        assert (tmp_path / "2026-03-02.jsonl").exists()


# ---------------------------------------------------------------------------
# log_previous_day_backup
# ---------------------------------------------------------------------------

class TestLogPreviousDayBackup:

    def test_does_nothing_if_no_file(self, tmp_path, caplog):
        with patch("upload.location.overland.backup.LOCATION_OVERLAND_BACKUP_DIR", tmp_path), \
             patch("upload.location.overland.backup.datetime") as mock_dt:
            # Yesterday = 2026-03-01, today = 2026-03-02
            mock_dt.now.return_value.__sub__ = lambda self, other: MagicMock(
                strftime=lambda fmt: "2026-03-01"
            )
            log_previous_day_backup()

        assert caplog.text == ""  # no logs emitted

    def test_does_nothing_if_file_is_empty(self, tmp_path, caplog):
        (tmp_path / "2026-03-01.jsonl").write_text("")
        with patch("upload.location.overland.backup.LOCATION_OVERLAND_BACKUP_DIR", tmp_path), \
             patch("upload.location.overland.backup.datetime") as mock_dt:
            from datetime import timedelta, timezone, datetime as real_dt
            yesterday = real_dt(2026, 3, 1, tzinfo=timezone.utc)
            mock_dt.now.return_value.__sub__ = MagicMock(return_value=yesterday)
            yesterday_mock = MagicMock()
            yesterday_mock.strftime.return_value = "2026-03-01"
            mock_dt.now.return_value.__sub__ = MagicMock(return_value=yesterday_mock)
            log_previous_day_backup()

        assert caplog.text == ""

    def test_logs_info_for_valid_file(self, tmp_path, caplog):
        import logging
        # Write a valid JSONL file with 2 payloads, 3 locations each
        payload = _make_payload(3)
        lines = [payload.model_dump_json(), payload.model_dump_json()]
        (tmp_path / "2026-03-01.jsonl").write_text("\n".join(lines) + "\n")

        with patch("upload.location.overland.backup.LOCATION_OVERLAND_BACKUP_DIR", tmp_path):
            # Patch datetime so "yesterday" resolves to 2026-03-01
            with patch("upload.location.overland.backup.datetime") as mock_dt:
                yesterday_mock = MagicMock()
                yesterday_mock.strftime.return_value = "2026-03-01"
                mock_dt.now.return_value.__sub__ = MagicMock(return_value=yesterday_mock)
                with caplog.at_level(logging.INFO, logger="upload.location.overland.backup"):
                    log_previous_day_backup()

        assert "2 payloads" in caplog.text
        assert "6 points" in caplog.text
