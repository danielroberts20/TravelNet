# conftest.py  (repo root)

import sys
import os
from unittest.mock import patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "server", "app"))

# Keep the test suite away from any real Prefect server.
#
# The developer's Prefect profile (~/.prefect/profiles.toml) points at the
# production server, so a test that runs a flow or task through the Prefect
# engine would silently write real flow runs, task runs and logs into production.
# (That happened: dozens of "Backfill FX" / "Backfill GBP" test runs ended up in the
# production Prefect UI.) Environment variables beat the profile, so point the
# client at a closed local port and forbid the ephemeral server: engine use now
# fails fast with a connection error instead. Call `flow.fn(...)` / `task.fn(...)`
# and patch `get_run_logger` in tests. Must run before anything imports prefect.
os.environ["PREFECT_API_URL"] = "http://127.0.0.1:9/api"
os.environ["PREFECT_SERVER_ALLOW_EPHEMERAL_MODE"] = "false"
os.environ["PREFECT_LOGGING_TO_API_ENABLED"] = "false"


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
        "upload.transaction.router.send_notification",
        "upload.transaction.wise_upload.send_notification",
        "upload.location.shortcuts.send_notification",
        "metadata.router.send_notification",
    ]
    patches = [patch(t) for t in targets]
    for p in patches:
        p.start()
    yield
    for p in patches:
        p.stop()