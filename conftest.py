# conftest.py  (repo root)

import sys
import os
from unittest.mock import patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "server", "app"))


@pytest.fixture(autouse=True)
def suppress_notifications():
    """Prevent any real Pushcut/HTTP notification calls during the test suite.

    Each module that does `from notifications import send_notification` gets its
    own local binding, so we must patch every known importer individually.
    Function scope ensures the patch is active for every test, including those
    that create a fresh TestClient (which fires the app startup event).
    """
    targets = [
        "notifications.send_notification",
        "main.send_notification",
        "database.transaction.ingest.revolut.send_notification",
        "database.transaction.ingest.wise.send_notification",
        "upload.transaction.endpoints.send_notification",
        "upload.utils.send_notification",
        "metadata.endpoints.send_notification",
    ]
    patches = [patch(t) for t in targets]
    for p in patches:
        p.start()
    yield
    for p in patches:
        p.stop()