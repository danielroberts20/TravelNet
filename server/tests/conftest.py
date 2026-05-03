import sys
import os

# Remove any Trevor paths from sys.path before importing anything
sys.path = [p for p in sys.path if "Trevor" not in p]
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "app")))

# Evict Trevor's config.py from the module cache
sys.modules.pop("config", None)

# Explicitly import TravelNet's config package now (not Trevor's config.py)
# This ensures the correct module is cached before patch() resolves it
import config          # noqa: E402
import config.logging  # noqa: E402

from unittest.mock import patch
with patch("config.logging.configure_logging"):
    import main  # noqa: F401
