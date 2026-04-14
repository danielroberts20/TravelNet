"""
test_flag_location_noise.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Unit tests for scheduled_tasks/flag_location_noise.py.

Covers flag_tier1_noise():
  - accuracy > threshold → flagged with tier=1, reason='accuracy_threshold'
  - accuracy ≤ threshold → not flagged
  - exactly at threshold → not flagged (condition is strictly >)
  - already-flagged point → skipped (NOT EXISTS guard)
  - returns count of newly flagged points

Covers flag_tier2_noise():
  - displacement spike (out-and-back within time window) → flagged tier=2
  - normal monotone movement → not flagged
  - displacement below TIER2_DISPLACEMENT_M → not flagged
  - next point does not return close enough → not flagged
  - prev→next window exceeds TIER2_WINDOW_S → not flagged
  - point already in location_noise → excluded by NOT EXISTS, spike undetectable
  - trailing TIER2_TRAILING_SKIP points not processed
  - fewer than 3 candidate rows → returns 0
  - multiple independent spikes in one pass → all flagged
"""

import sqlite3
import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch, MagicMock

from scheduled_tasks.flag_location_noise import flag_tier1_noise, flag_tier2_noise


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript("""
        CREATE TABLE location_overland (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id           TEXT NOT NULL DEFAULT 'iphone',
            timestamp           TEXT NOT NULL,
            latitude            REAL NOT NULL,
            longitude           REAL NOT NULL,
            altitude            REAL,
            speed               REAL,
            horizontal_accuracy REAL,
            vertical_accuracy   REAL,
            motion              TEXT,
            activity            TEXT,
            wifi_ssid           TEXT,
            battery_state       TEXT,
            battery_level       REAL,
            pauses              INTEGER,
            desired_accuracy    REAL,
            significant_change  TEXT,
            place_id            INTEGER,
            raw_json            TEXT,
            inserted_at         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
            UNIQUE(device_id, timestamp)
        );

        CREATE TABLE location_noise (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            overland_id INTEGER NOT NULL REFERENCES location_overland(id) ON DELETE CASCADE,
            tier        INTEGER NOT NULL,
            reason      TEXT NOT NULL,
            flagged_at  TEXT NOT NULL DEFAULT(strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
        );

        CREATE UNIQUE INDEX idx_noise_overland_id ON location_noise(overland_id);
    """)
    return conn


def _insert_point(conn, ts, lat, lon, h_acc=10.0):
    conn.execute(
        "INSERT INTO location_overland (timestamp, latitude, longitude, horizontal_accuracy)"
        " VALUES (?, ?, ?, ?)",
        (ts, lat, lon, h_acc),
    )
    return conn.execute("SELECT last_insert_rowid()").fetchone()[0]


def _flag_noise(conn, overland_id, tier=1, reason="accuracy_threshold"):
    conn.execute(
        "INSERT INTO location_noise (overland_id, tier, reason) VALUES (?, ?, ?)",
        (overland_id, tier, reason),
    )


# ---------------------------------------------------------------------------
# TestFlagTier1Noise
# ---------------------------------------------------------------------------

class TestFlagTier1Noise:

    @pytest.fixture(autouse=True)
    def mock_logger(self):
        with patch("scheduled_tasks.flag_location_noise.get_run_logger") as m:
            m.return_value = MagicMock()
            yield m

    @pytest.fixture(autouse=True)
    def patch_noise_conn(self, db):
        with patch("database.location.noise.table.get_conn", return_value=db):
            yield

    def test_flags_high_accuracy_point(self, db):
        _insert_point(db, "2024-06-15T09:00:00Z", 48.8566, 2.3522, h_acc=150.0)
        count = flag_tier1_noise.fn(db)
        assert count == 1
        row = db.execute("SELECT tier, reason FROM location_noise LIMIT 1").fetchone()
        assert row["tier"] == 1
        assert row["reason"] == "accuracy_threshold"

    def test_skips_low_accuracy_point(self, db):
        _insert_point(db, "2024-06-15T09:00:00Z", 48.8566, 2.3522, h_acc=50.0)
        count = flag_tier1_noise.fn(db)
        assert count == 0
        assert db.execute("SELECT COUNT(*) FROM location_noise").fetchone()[0] == 0

    def test_skips_boundary_accuracy(self, db):
        """Exactly at threshold (100 m) is not flagged — condition is strictly >."""
        _insert_point(db, "2024-06-15T09:00:00Z", 48.8566, 2.3522, h_acc=100.0)
        count = flag_tier1_noise.fn(db)
        assert count == 0

    def test_skips_already_flagged_point(self, db):
        oid = _insert_point(db, "2024-06-15T09:00:00Z", 48.8566, 2.3522, h_acc=150.0)
        _flag_noise(db, oid)
        count = flag_tier1_noise.fn(db)
        assert count == 0

    def test_returns_count_of_newly_flagged(self, db):
        _insert_point(db, "2024-06-15T09:00:00Z", 48.8566, 2.3522, h_acc=200.0)
        _insert_point(db, "2024-06-15T10:00:00Z", 48.8567, 2.3523, h_acc=300.0)
        _insert_point(db, "2024-06-15T11:00:00Z", 48.8568, 2.3524, h_acc=50.0)   # under threshold
        count = flag_tier1_noise.fn(db)
        assert count == 2


# ---------------------------------------------------------------------------
# TestFlagTier2Noise
# ---------------------------------------------------------------------------

# Displacement spike scenario
# ─────────────────────────────────────────────────────────────────────────
# A (origin) → B (spikes far away) → C (returns to A) — B is noise.
# At the equator 0.002° of longitude ≈ 222 m:
#   dist A→B ≈ 222 m  > TIER2_DISPLACEMENT_M (150 m)  ✓
#   dist C→A ≈   0 m  < TIER2_RETURN_M       (150 m)  ✓
#   window A→C = 20 s < TIER2_WINDOW_S        (30 s)   ✓

_BASE = datetime(2024, 6, 15, 9, 0, 0, tzinfo=timezone.utc)


def _ts(offset_s: int) -> str:
    return (_BASE + timedelta(seconds=offset_s)).strftime("%Y-%m-%dT%H:%M:%SZ")


class TestFlagTier2Noise:

    @pytest.fixture(autouse=True)
    def patch_noise_conn(self, db):
        with patch("database.location.noise.table.get_conn", return_value=db):
            yield

    @pytest.fixture(autouse=True)
    def minimal_trailing_skip(self):
        """Reduce trailing skip to 1 so tests only need one extra padding row."""
        with patch("scheduled_tasks.flag_location_noise.TIER2_TRAILING_SKIP", 1):
            yield

    def _spike_sequence(self, db, extra_trailing: int = 1):
        """Insert A → B(spike) → C(return) + `extra_trailing` padding rows."""
        _insert_point(db, _ts(0),  0.0, 0.0000)   # A origin
        _insert_point(db, _ts(10), 0.0, 0.0020)   # B spike  ≈ 222 m from A
        _insert_point(db, _ts(20), 0.0, 0.0000)   # C return to origin
        for i in range(extra_trailing):
            _insert_point(db, _ts(30 + i * 10), 0.0, 0.0001)

    def test_flags_displacement_spike(self, db):
        self._spike_sequence(db)
        count = flag_tier2_noise.fn(db)
        assert count == 1
        row = db.execute("SELECT tier, reason FROM location_noise LIMIT 1").fetchone()
        assert row["tier"] == 2
        assert row["reason"] == "displacement_spike"

    def test_does_not_flag_normal_movement(self, db):
        """Monotone trajectory (no return) is not flagged."""
        _insert_point(db, _ts(0),  0.0, 0.0000)
        _insert_point(db, _ts(10), 0.0, 0.0010)   # ~111 m step — under threshold
        _insert_point(db, _ts(20), 0.0, 0.0020)   # continues forward
        _insert_point(db, _ts(30), 0.0, 0.0030)   # padding
        count = flag_tier2_noise.fn(db)
        assert count == 0

    def test_displacement_below_threshold_not_flagged(self, db):
        """Spike displacement < TIER2_DISPLACEMENT_M (150 m) — not flagged."""
        _insert_point(db, _ts(0),  0.0, 0.0000)
        _insert_point(db, _ts(10), 0.0, 0.0010)   # ~111 m from A — under 150 m
        _insert_point(db, _ts(20), 0.0, 0.0000)   # returns
        _insert_point(db, _ts(30), 0.0, 0.0000)   # padding
        count = flag_tier2_noise.fn(db)
        assert count == 0

    def test_return_distance_too_large_not_flagged(self, db):
        """Spike but next point does not return close enough to prev."""
        _insert_point(db, _ts(0),  0.0, 0.0000)   # A
        _insert_point(db, _ts(10), 0.0, 0.0020)   # B spike
        _insert_point(db, _ts(20), 0.0, 0.0015)   # C — ~167 m from A, > TIER2_RETURN_M
        _insert_point(db, _ts(30), 0.0, 0.0015)   # padding
        count = flag_tier2_noise.fn(db)
        assert count == 0

    def test_outside_time_window_not_flagged(self, db):
        """Spike with correct distances but prev→next elapsed > TIER2_WINDOW_S (30 s)."""
        _insert_point(db, _ts(0),  0.0, 0.0000)   # A
        _insert_point(db, _ts(10), 0.0, 0.0020)   # B spike
        _insert_point(db, _ts(40), 0.0, 0.0000)   # C return — 40 s after A, > 30 s
        _insert_point(db, _ts(50), 0.0, 0.0000)   # padding
        count = flag_tier2_noise.fn(db)
        assert count == 0

    def test_excludes_tier1_flagged_spike_candidate(self, db):
        """A spike candidate already in location_noise is excluded by NOT EXISTS,
        so the surrounding points no longer form a detectable spike pattern."""
        oid_b = None
        _insert_point(db, _ts(0),  0.0, 0.0000)          # A
        oid_b = _insert_point(db, _ts(10), 0.0, 0.0020)  # B spike candidate
        _insert_point(db, _ts(20), 0.0, 0.0000)           # C return
        _insert_point(db, _ts(30), 0.0, 0.0000)           # padding
        _flag_noise(db, oid_b, tier=1, reason="accuracy_threshold")
        count = flag_tier2_noise.fn(db)
        assert count == 0

    def test_too_few_rows_returns_zero(self, db):
        """Fewer than 3 candidate rows → returns 0 without error."""
        _insert_point(db, _ts(0),  0.0, 0.0000)
        _insert_point(db, _ts(10), 0.0, 0.0020)
        count = flag_tier2_noise.fn(db)
        assert count == 0

    def test_trailing_skip_protects_recent_spike(self, db):
        """A spike that falls within the trailing skip window is not processed."""
        with patch("scheduled_tasks.flag_location_noise.TIER2_TRAILING_SKIP", 3):
            # 5 rows: [A, pad, C(spike), D(return), E(trailing)]
            # After rows[:-3] the kept set is [A, pad] — C is never reached.
            _insert_point(db, _ts(0),  0.0, 0.0000)   # A
            _insert_point(db, _ts(10), 0.0, 0.0000)   # pad
            _insert_point(db, _ts(20), 0.0, 0.0020)   # C spike (within trailing window)
            _insert_point(db, _ts(30), 0.0, 0.0000)   # D return
            _insert_point(db, _ts(40), 0.0, 0.0000)   # E trailing
            count = flag_tier2_noise.fn(db)
            assert count == 0

    def test_multiple_spikes_all_flagged(self, db):
        """Two independent out-and-back spikes in one pass are both flagged."""
        # Spike 1
        _insert_point(db, _ts(0),  0.0, 0.0000)
        _insert_point(db, _ts(10), 0.0, 0.0020)   # spike 1
        _insert_point(db, _ts(20), 0.0, 0.0000)
        # Spike 2
        _insert_point(db, _ts(30), 0.0, 0.0000)
        _insert_point(db, _ts(40), 0.0, 0.0020)   # spike 2
        _insert_point(db, _ts(50), 0.0, 0.0000)
        # Padding
        _insert_point(db, _ts(60), 0.0, 0.0000)
        count = flag_tier2_noise.fn(db)
        assert count == 2
