"""
config/runtime.py
~~~~~~~~~~~~~~~~~
Records the moment the application process started.

Imported once at startup (in main.py) so that app_start_time is stable for
the lifetime of the container.  Used by metadata/util.py to compute the
application uptime displayed on the /metadata/status endpoint.
"""

from datetime import datetime, timezone


app_start_time = datetime.now(tz=timezone.utc)
