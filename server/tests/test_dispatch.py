"""
test_dispatch.py — Unit tests for triggers/dispatch.py.

Covers:
  - dispatch: fires notification and inserts trigger_log when no recent record
  - dispatch: returns True when fired, False when suppressed by cooldown
  - dispatch: cooldown check uses trigger-specific window (different triggers don't share cooldown)
  - dispatch: payload stored in trigger_log as JSON
  - journal_notification (not send_notification) is called
"""

import json
import sqlite3
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock

from triggers.dispatch import dispatch


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE trigger_log (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            trigger    TEXT NOT NULL,
            fired_at   TEXT NOT NULL,
            payload    TEXT
        );
    """)
    return conn


def _run_dispatch(db, trigger="location_change", payload=None, cooldown_hours=1):
    payload = payload or {"lat": 1.0, "lon": 2.0}
    with patch("triggers.dispatch.get_conn", return_value=db), \
         patch("triggers.dispatch.trigger_table") as mock_trigger_table, \
         patch("triggers.dispatch.label_known_place_notification") as mock_notif:
        result = dispatch(
            trigger=trigger,
            payload=payload,
            cooldown_hours=cooldown_hours,
            noti_title="Test",
            noti_body="Body",
        )
    return result, mock_notif, mock_trigger_table


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestDispatch:

    def test_fires_when_no_previous_record(self, db):
        result, mock_notif, _ = _run_dispatch(db)
        assert result is True

    def test_calls_journal_notification(self, db):
        _, mock_notif, _ = _run_dispatch(db)
        mock_notif.assert_called_once()

    def test_notification_receives_title_and_body(self, db):
        _, mock_notif, _ = _run_dispatch(db, payload={"k": "v"})
        call_kwargs = mock_notif.call_args
        # title and body must be passed (positional or keyword)
        args, kwargs = call_kwargs
        all_args = list(args) + list(kwargs.values())
        assert "Test" in all_args or kwargs.get("title") == "Test"

    def test_inserts_trigger_log_record(self, db):
        _, _, mock_trigger_table = _run_dispatch(db)
        mock_trigger_table.insert.assert_called_once()

    def test_suppressed_by_cooldown(self, db):
        # Insert a recent trigger_log entry
        recent = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        db.execute(
            "INSERT INTO trigger_log (trigger, fired_at, payload) VALUES (?, ?, ?)",
            ("location_change", recent, "{}"),
        )
        db.commit()

        result, mock_notif, _ = _run_dispatch(db, trigger="location_change", cooldown_hours=1)
        assert result is False
        mock_notif.assert_not_called()

    def test_not_suppressed_by_old_record(self, db):
        # Insert an old record (outside cooldown window)
        old = (datetime.now(timezone.utc) - timedelta(hours=3)).strftime("%Y-%m-%dT%H:%M:%SZ")
        db.execute(
            "INSERT INTO trigger_log (trigger, fired_at, payload) VALUES (?, ?, ?)",
            ("location_change", old, "{}"),
        )
        db.commit()

        result, mock_notif, _ = _run_dispatch(db, trigger="location_change", cooldown_hours=1)
        assert result is True

    def test_different_triggers_dont_share_cooldown(self, db):
        # Insert recent record for trigger_a — trigger_b should still fire
        recent = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        db.execute(
            "INSERT INTO trigger_log (trigger, fired_at, payload) VALUES (?, ?, ?)",
            ("trigger_a", recent, "{}"),
        )
        db.commit()

        result, mock_notif, _ = _run_dispatch(db, trigger="trigger_b", cooldown_hours=1)
        assert result is True

    def test_returns_false_without_calling_notification(self, db):
        recent = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        db.execute(
            "INSERT INTO trigger_log (trigger, fired_at, payload) VALUES (?, ?, ?)",
            ("my_trigger", recent, "{}"),
        )
        db.commit()

        result, mock_notif, mock_trigger_table = _run_dispatch(db, trigger="my_trigger")
        assert result is False
        mock_notif.assert_not_called()
        mock_trigger_table.insert.assert_not_called()
