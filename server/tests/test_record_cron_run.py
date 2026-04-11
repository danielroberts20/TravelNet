"""
test_record_cron_run.py — Unit tests for notifications._record_cron_run().

Covers:
  - Creates cron_runs.json when file does not exist
  - Writes correct fields: success, detail, timestamp, ts_human
  - Merges with existing data (other jobs preserved)
  - Overwrites existing entry for same job
  - Handles corrupted JSON gracefully (treats as empty)
  - Uses atomic write: tmp file renamed to final path
"""

import json
import pytest
from unittest.mock import patch

from notifications import _record_cron_run


@pytest.fixture
def data_dir(tmp_path):
    return tmp_path


def _run(data_dir, job="test_job", success=True, detail=""):
    # DATA_DIR is imported locally inside _record_cron_run via `from config.general import DATA_DIR`
    with patch("config.general.DATA_DIR", data_dir):
        _record_cron_run(job, success=success, detail=detail)


def _read(data_dir):
    return json.loads((data_dir / "cron_runs.json").read_text())


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestRecordCronRun:

    def test_creates_file_when_missing(self, data_dir):
        _run(data_dir)
        assert (data_dir / "cron_runs.json").exists()

    def test_written_entry_has_required_fields(self, data_dir):
        _run(data_dir, job="my_job", success=True, detail="ok")
        data = _read(data_dir)
        entry = data["my_job"]
        assert "success" in entry
        assert "detail" in entry
        assert "timestamp" in entry
        assert "ts_human" in entry

    def test_success_true_stored(self, data_dir):
        _run(data_dir, job="j", success=True)
        assert _read(data_dir)["j"]["success"] is True

    def test_success_false_stored(self, data_dir):
        _run(data_dir, job="j", success=False)
        assert _read(data_dir)["j"]["success"] is False

    def test_detail_stored(self, data_dir):
        _run(data_dir, job="j", success=False, detail="something went wrong")
        assert _read(data_dir)["j"]["detail"] == "something went wrong"

    def test_timestamp_is_integer(self, data_dir):
        _run(data_dir)
        entry = _read(data_dir)["test_job"]
        assert isinstance(entry["timestamp"], int)

    def test_ts_human_is_string(self, data_dir):
        _run(data_dir)
        entry = _read(data_dir)["test_job"]
        assert isinstance(entry["ts_human"], str)
        assert "UTC" in entry["ts_human"]

    def test_merges_with_existing_data(self, data_dir):
        # Pre-seed with another job
        existing = {"other_job": {"success": True, "detail": "", "timestamp": 1000, "ts_human": "old"}}
        (data_dir / "cron_runs.json").write_text(json.dumps(existing))

        _run(data_dir, job="new_job", success=True)
        data = _read(data_dir)
        assert "other_job" in data
        assert "new_job" in data

    def test_overwrites_same_job(self, data_dir):
        existing = {"test_job": {"success": False, "detail": "old", "timestamp": 1, "ts_human": "old"}}
        (data_dir / "cron_runs.json").write_text(json.dumps(existing))

        _run(data_dir, job="test_job", success=True, detail="new")
        data = _read(data_dir)
        assert data["test_job"]["success"] is True
        assert data["test_job"]["detail"] == "new"
        assert data["test_job"]["timestamp"] > 1

    def test_corrupted_json_treated_as_empty(self, data_dir):
        (data_dir / "cron_runs.json").write_text("{not valid json{{")
        # Should not raise
        _run(data_dir, job="j", success=True)
        data = _read(data_dir)
        assert "j" in data

    def test_no_tmp_file_left_behind(self, data_dir):
        _run(data_dir)
        assert not (data_dir / "cron_runs.tmp").exists()
