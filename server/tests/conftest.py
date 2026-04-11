"""
server/tests/conftest.py
~~~~~~~~~~~~~~~~~~~~~~~~
Ensures main.py is importable for all tests in this directory.

The root conftest.py's suppress_notifications fixture patches
main.send_notification, which requires main to already be in sys.modules.
The transaction/conftest.py handles this for tests under transaction/; this
file provides the same guarantee for all other tests in server/tests/.
"""

from unittest.mock import patch

with patch("config.logging.configure_logging"):
    import main  # noqa: F401
