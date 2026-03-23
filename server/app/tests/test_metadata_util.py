"""
test_metadata_util.py — Unit tests for metadata/util.py pure helpers.

Covers:
  - _format_uptime: seconds → 'Xd Yh Zm' string
  - _backup_info: stat-based dict for a single file
  - _latest_in_dir: most-recently-modified file in a directory
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from metadata.util import _backup_info, _format_uptime, _latest_in_dir


# ---------------------------------------------------------------------------
# _format_uptime
# ---------------------------------------------------------------------------

class TestFormatUptime:

    @pytest.mark.parametrize("seconds, expected", [
        (0,       "0d 0h 0m"),
        (60,      "0d 0h 1m"),
        (3600,    "0d 1h 0m"),
        (3661,    "0d 1h 1m"),
        (86400,   "1d 0h 0m"),
        (90061,   "1d 1h 1m"),   # 1d + 1h + 1m + 1s (seconds truncated)
        (172800,  "2d 0h 0m"),
    ])
    def test_format(self, seconds, expected):
        assert _format_uptime(seconds) == expected

    def test_float_input_truncated_not_rounded(self):
        # 3661.9 seconds = 1h 1m 1.9s → displayed as "0d 1h 1m"
        assert _format_uptime(3661.9) == "0d 1h 1m"


# ---------------------------------------------------------------------------
# _backup_info
# ---------------------------------------------------------------------------

class TestBackupInfo:

    def _mock_path(self, name: str, size: int, mtime: float) -> MagicMock:
        p = MagicMock(spec=Path)
        p.name = name
        stat = MagicMock()
        stat.st_size = size
        stat.st_mtime = mtime
        p.stat.return_value = stat
        return p

    def test_returns_dict_with_expected_keys(self):
        p = self._mock_path("travel.db", 1024 * 1024, 1_700_000_000.0)
        result = _backup_info(p)
        assert result is not None
        assert "filename" in result
        assert "size_mb" in result
        assert "modified_ts" in result
        assert "modified" in result
        assert "stale" in result

    def test_filename_matches_path_name(self):
        p = self._mock_path("travel.db", 512, 1_700_000_000.0)
        assert _backup_info(p)["filename"] == "travel.db"

    def test_size_mb_calculated_correctly(self):
        p = self._mock_path("f.db", 2 * 1024 * 1024, 1_700_000_000.0)
        assert _backup_info(p)["size_mb"] == pytest.approx(2.0)

    def test_modified_ts_is_int(self):
        p = self._mock_path("f.db", 100, 1_700_000_000.0)
        assert _backup_info(p)["modified_ts"] == 1_700_000_000

    def test_returns_none_on_stat_error(self):
        p = MagicMock(spec=Path)
        p.stat.side_effect = FileNotFoundError("no file")
        assert _backup_info(p) is None

    def test_recent_file_not_stale(self):
        import time
        recent_mtime = time.time() - 60  # 1 minute ago
        p = self._mock_path("recent.db", 100, recent_mtime)
        result = _backup_info(p)
        assert result["stale"] is False

    def test_old_file_is_stale(self):
        import time
        old_mtime = time.time() - (8 * 86400)  # 8 days ago (> STALE_DAYS=7)
        p = self._mock_path("old.db", 100, old_mtime)
        result = _backup_info(p)
        assert result["stale"] is True


# ---------------------------------------------------------------------------
# _latest_in_dir
# ---------------------------------------------------------------------------

class TestLatestInDir:

    def test_returns_none_for_empty_directory(self, tmp_path):
        result = _latest_in_dir(tmp_path, "*.db")
        assert result is None

    def test_returns_info_for_single_file(self, tmp_path):
        f = tmp_path / "backup.db"
        f.write_bytes(b"x" * 1024)
        result = _latest_in_dir(tmp_path, "*.db")
        assert result is not None
        assert result["filename"] == "backup.db"

    def test_count_reflects_total_files(self, tmp_path):
        for i in range(3):
            (tmp_path / f"backup_{i}.db").write_bytes(b"x" * 100)
        result = _latest_in_dir(tmp_path, "*.db")
        assert result["count"] == 3

    def test_returns_most_recently_modified_file(self, tmp_path):
        import time
        older = tmp_path / "older.db"
        older.write_bytes(b"x")
        # Ensure a different mtime by touching the newer file after
        newer = tmp_path / "newer.db"
        newer.write_bytes(b"y")
        # Force mtimes explicitly
        older_ts = time.time() - 200
        newer_ts = time.time() - 10
        import os
        os.utime(older, (older_ts, older_ts))
        os.utime(newer, (newer_ts, newer_ts))

        result = _latest_in_dir(tmp_path, "*.db")
        assert result["filename"] == "newer.db"

    def test_pattern_filters_files(self, tmp_path):
        (tmp_path / "data.db").write_bytes(b"x")
        (tmp_path / "data.csv").write_bytes(b"y")
        result = _latest_in_dir(tmp_path, "*.csv")
        assert result is not None
        assert result["filename"] == "data.csv"

    def test_no_match_for_pattern_returns_none(self, tmp_path):
        (tmp_path / "data.db").write_bytes(b"x")
        result = _latest_in_dir(tmp_path, "*.zip")
        assert result is None
