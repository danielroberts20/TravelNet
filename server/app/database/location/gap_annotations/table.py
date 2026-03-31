"""
database/location/gap_annotations/table.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Persistence layer for user-supplied gap annotations.

A gap annotation records a known period when data was unavailable (e.g. phone
in for repair, flight mode) along with a human-readable description.  When
gap-detection logic finds a gap in location or health data it can call
is_gap_covered() to check whether a matching annotation already explains it.

The tolerance window used by is_gap_covered() is driven by the editable config
GAP_ANNOTATION_TOLERANCE_MINUTES (default 10 min) so the user does not need to
know the exact second that the gap started or ended.
"""

from typing import Optional

from database.connection import get_conn, to_iso_str
from config.general import GAP_ANNOTATION_TOLERANCE_MINUTES


def init() -> None:
    """Create the gap_annotations table and its indexes if they do not exist."""
    with get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS gap_annotations (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                start_ts    TEXT NOT NULL,
                end_ts      TEXT NOT NULL, 
                description TEXT    NOT NULL,
                created_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
            );
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_gap_annotations_start
                ON gap_annotations(start_ts);
        """)


def insert_annotation(start_ts: int, end_ts: int, description: str) -> int:
    """Insert a new gap annotation and return its auto-assigned row id."""
    new_start = to_iso_str(start_ts)
    new_end = to_iso_str(end_ts)
    with get_conn() as conn:
        cursor = conn.execute(
            """
            INSERT INTO gap_annotations (start_ts, end_ts, description)
            VALUES (?, ?, ?)
            """,
            (new_start, new_end, description),
        )
        conn.commit()
        return cursor.lastrowid


def is_gap_covered(
    gap_start: int,
    gap_end: int,
    tolerance_minutes: Optional[int] = None,
) -> Optional[dict]:
    """Check whether a gap [gap_start, gap_end] is explained by any annotation.

    An annotation is considered to cover a gap when the annotation window,
    expanded by ±tolerance on each side, fully contains the gap:

        ann_start - tolerance_s  <=  gap_start
        ann_end   + tolerance_s  >=  gap_end

    Parameters
    ----------
    gap_start:          Unix timestamp (seconds) of gap start.
    gap_end:            Unix timestamp (seconds) of gap end.
    tolerance_minutes:  Override the default tolerance.  If None the current
                        value of GAP_ANNOTATION_TOLERANCE_MINUTES is used.

    Returns the first matching annotation as a dict, or None if no annotation
    covers the gap.
    """
    if tolerance_minutes is None:
        # Import here to pick up any runtime override that was loaded at startup
        from config.editable import get_value
        tolerance_minutes = get_value(
            "GAP_ANNOTATION_TOLERANCE_MINUTES", GAP_ANNOTATION_TOLERANCE_MINUTES
        )

    tolerance_s = int(tolerance_minutes) * 60

    with get_conn(read_only=True) as conn:
        row = conn.execute(
            """
            SELECT id, start_ts, end_ts, description, created_at
            FROM gap_annotations
            WHERE (start_ts - :tol) <= :gap_start
              AND (end_ts   + :tol) >= :gap_end
            LIMIT 1
            """,
            {"tol": tolerance_s, "gap_start": gap_start, "gap_end": gap_end},
        ).fetchone()

    return dict(row) if row else None


def list_annotations() -> list[dict]:
    """Return all gap annotations ordered by start time (ascending)."""
    with get_conn(read_only=True) as conn:
        rows = conn.execute(
            """
            SELECT id, start_ts, end_ts, description, created_at
            FROM gap_annotations
            ORDER BY start_ts ASC
            """
        ).fetchall()
    return [dict(row) for row in rows]
