# tests/test_logging_config.py

import logging
from unittest.mock import patch, MagicMock
import pytest
from config.logging import DailyDigestHandler


@pytest.fixture
def handler():
    """Isolated DailyDigestHandler with DB init mocked out."""
    with patch("config.logging.get_conn"):
        h = DailyDigestHandler()
    return h


def test_digest_sends_on_warn(handler):
    # Emit directly rather than via a real logger to avoid global state
    handler.emit(logging.makeLogRecord({"levelno": logging.WARNING, "levelname": "WARNING", "msg": "something went wrong", "name": "test", "module": "test", "lineno": 1}))
    handler.emit(logging.makeLogRecord({"levelno": logging.ERROR, "levelname": "ERROR", "msg": "something went badly wrong", "name": "test", "module": "test", "lineno": 2}))

    with patch("database.logging.digest.table.get_conn") as mock_conn, patch("smtplib.SMTP") as mock_smtp:
        # Mock DB returning 2 rows
        mock_conn.return_value.__enter__.return_value.execute.return_value.fetchall.return_value = [
            ("2026-03-01 06:00:00 UTC", "WARNING", "test", "test", 1, "something went wrong"),
            ("2026-03-01 06:00:01 UTC", "ERROR", "test", "test", 2, "something went badly wrong"),
        ]
        instance = mock_smtp.return_value.__enter__.return_value
        handler.flush_and_send("smtp.gmail.com", 587, "a@b.com", "pw", "a@b.com", "user")

    assert instance.send_message.called
    sent_msg = instance.send_message.call_args[0][0]
    assert "2 alert(s)" in sent_msg["Subject"]
    assert "something went wrong" in sent_msg.get_content()
    assert "something went badly wrong" in sent_msg.get_content()


def test_digest_silent_when_empty(handler):
    with patch("database.logging.digest.table.get_conn") as mock_conn, patch("smtplib.SMTP") as mock_smtp:
        mock_conn.return_value.__enter__.return_value.execute.return_value.fetchall.return_value = []
        handler.flush_and_send("smtp.gmail.com", 587, "a@b.com", "pw", "a@b.com", "user")
        mock_smtp.assert_not_called()