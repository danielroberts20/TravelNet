"""
nearest_place.py
~~~~~~~~~~~~~~~~
Find the place_id of the location fix nearest to a timestamp.

Used by the Backfill Place flow to attach a place to health / transaction /
photo rows. Stdlib only, so it can be imported and tested without Prefect or
application settings.

Two implementations with identical results:

* The fast path queries the two base tables (location_overland and
  location_shortcuts) with an indexable ``timestamp BETWEEN :lo AND :hi``
  range. It requires the query timestamp to be in the canonical stored form
  ``YYYY-MM-DDTHH:MM:SSZ`` so that string comparison equals time comparison.
* The legacy path queries the location_unified view with epoch arithmetic.
  SQLite has to materialise the whole view and scan it (≈1 s per lookup on
  ~420 k rows), so it is only used for timestamps in some other format.

The fast path reproduces the view's rule for dropping shortcuts points
(a shortcuts row is ignored when an overland row falls inside the same
datetime(...)±3 minute BETWEEN the view uses) verbatim, so the two paths agree.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta

_ISO_Z = "%Y-%m-%dT%H:%M:%SZ"
_CANONICAL = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")

# Legacy: scans the whole location_unified view (slow).
NEAREST_PLACE_VIEW_SQL = """
    SELECT lu.place_id FROM location_unified lu
    WHERE lu.place_id IS NOT NULL
        AND CAST(strftime('%s', lu.timestamp) AS INTEGER)
            BETWEEN CAST(strftime('%s', :ts) AS INTEGER) - :window_s
                AND CAST(strftime('%s', :ts) AS INTEGER) + :window_s
    ORDER BY
        CASE WHEN lu.timestamp <= :ts THEN 0 ELSE 1 END ASC,
        ABS(strftime('%s', lu.timestamp) - strftime('%s', :ts)) ASC
    LIMIT 1
"""

# Fast: index range scans on the base tables, unioned.
NEAREST_PLACE_RANGE_SQL = """
    SELECT u.place_id FROM (
        SELECT o.timestamp, o.place_id
        FROM location_overland o
        WHERE o.place_id IS NOT NULL
          AND o.timestamp BETWEEN :lo AND :hi
        UNION ALL
        SELECT s.timestamp, s.place_id
        FROM location_shortcuts s
        WHERE s.place_id IS NOT NULL
          AND s.timestamp BETWEEN :lo AND :hi
          AND NOT EXISTS (
              SELECT 1 FROM location_overland o2
              WHERE o2.timestamp BETWEEN
                  datetime(s.timestamp, '-3 minutes') AND
                  datetime(s.timestamp, '+3 minutes')
          )
    ) u
    ORDER BY
        CASE WHEN u.timestamp <= :ts THEN 0 ELSE 1 END ASC,
        ABS(strftime('%s', u.timestamp) - strftime('%s', :ts)) ASC
    LIMIT 1
"""


def nearest_place_id(conn, ts: str, window_s: int) -> int | None:
    """Return the place_id of the nearest location fix to ``ts`` within ``window_s`` seconds.

    Prefers the most recent fix at or before ``ts``; falls back to the earliest
    after it. Fixes without a place_id are ignored. ``conn`` needs
    ``row_factory = sqlite3.Row``.
    """
    if _CANONICAL.match(ts):
        centre = datetime.strptime(ts, _ISO_Z)
        window = timedelta(seconds=window_s)
        row = conn.execute(NEAREST_PLACE_RANGE_SQL, {
            "ts": ts,
            "lo": (centre - window).strftime(_ISO_Z),
            "hi": (centre + window).strftime(_ISO_Z),
        }).fetchone()
    else:
        row = conn.execute(NEAREST_PLACE_VIEW_SQL, {"ts": ts, "window_s": window_s}).fetchone()
    return row["place_id"] if row else None
