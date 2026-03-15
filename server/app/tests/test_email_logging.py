import logging
from unittest.mock import patch

from config.logging import DailyDigestHandler


def test_email():
    handler = DailyDigestHandler()
    logger = logging.getLogger("test")
    logger.addHandler(handler)

    logger.warning("something went wrong")
    logger.error("something went badly wrong")

    with patch("smtplib.SMTP") as mock_smtp:
        instance = mock_smtp.return_value.__enter__.return_value
        handler.flush_and_send("smtp.gmail.com", 587, "a@b.com", "pw", "a@b.com")
        assert instance.send_message.called
        sent_msg = instance.send_message.call_args[0][0]
        assert "2 alert(s)" in sent_msg["Subject"]
        assert "something went wrong" in sent_msg.get_content()
        assert "something went badly wrong" in sent_msg.get_content()

def test_digest_silent_when_empty():
    handler = DailyDigestHandler()
    with patch("smtplib.SMTP") as mock_smtp:
        handler.flush_and_send("smtp.gmail.com", 587, "a@b.com", "pw", "a@b.com")
        mock_smtp.assert_not_called()