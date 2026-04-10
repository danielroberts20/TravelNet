"""
scheduled_tasks/detect_timezone_transitions.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Scans location_unified (joined to places) in chronological order and records
rows into timezone_transitions whenever the IANA timezone changes.

Safe to re-run — uses INSERT OR IGNORE on (transitioned_at, to_tz).

Run via:
    cd server && ./runcron.sh scheduled_tasks.detect_timezone_transitions
"""

import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from database.transition.timezone.table import table as transition_timezone_table, TransitionTimezoneRecord
from config.logging import configure_logging
from database.connection import get_conn
from notifications import CronJobMailer, DailyCronJobMailer
from config.settings import settings

logger = logging.getLogger(__name__)

# Fetch rows in pages to avoid loading millions of rows into memory at once.
PAGE_SIZE = 2000


# ---------------------------------------------------------------------------
# UTC offset helper
# ---------------------------------------------------------------------------

def _utc_offset_str(iana_tz: str, at_utc_iso: str) -> str | None:
    """
    Return the UTC offset for *iana_tz* at the moment *at_utc_iso* as a
    string like "+11:00" or "-05:30".

    Returns None if the IANA timezone string cannot be resolved (e.g. tzdata
    package missing, or an unrecognised zone name stored in places).
    """
    try:
        tz = ZoneInfo(iana_tz)
    except (ZoneInfoNotFoundError, KeyError):
        logger.warning("Unrecognised IANA timezone: %s", iana_tz)
        return None

    # Normalise the ISO string — SQLite stores UTC as "...Z" or "+00:00".
    ts = at_utc_iso.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(ts).astimezone(timezone.utc)
    except ValueError:
        logger.warning("Could not parse timestamp: %s", at_utc_iso)
        return None

    offset = dt.astimezone(tz).utcoffset()
    total_minutes = int(offset.total_seconds() / 60)
    sign = "+" if total_minutes >= 0 else "-"
    h, m = divmod(abs(total_minutes), 60)
    return f"{sign}{h:02d}:{m:02d}"


# ---------------------------------------------------------------------------
# Core detection logic
# ---------------------------------------------------------------------------

def _detect_transitions(conn) -> dict:
    """
    Walk location_unified joined to places in chronological order.
    Insert a timezone_transitions row each time places.timezone changes.

    Returns a dict with diagnostic counts.
    """
    inserted = 0
    skipped = 0       # INSERT OR IGNORE hits (already recorded)
    null_skipped = 0  # rows where place has no timezone yet

    current_tz: str | None = None
    current_offset: str | None = None

    # Use an offset-based page cursor so we don't hold a giant result set.
    # We ORDER BY timestamp ASC consistently across pages.
    offset = 0

    while True:
        rows = conn.execute("""
            SELECT
                u.timestamp,
                p.timezone,
                p.id AS place_id
            FROM location_unified u
            JOIN places p ON u.place_id = p.id
            WHERE p.timezone IS NOT NULL
              AND p.timezone != ''
            ORDER BY u.timestamp ASC
            LIMIT ? OFFSET ?
        """, (PAGE_SIZE, offset)).fetchall()

        if not rows:
            break

        for row in rows:
            row_tz = row["timezone"]

            # Same timezone as before — no transition, keep walking.
            if row_tz == current_tz:
                continue

            # Timezone changed (or this is the very first geocoded point).
            ts = row["timestamp"]
            to_offset = _utc_offset_str(row_tz, ts)

            if to_offset is None:
                # Unresolvable timezone — skip without updating current_tz so
                # we don't lose track of the last known good timezone.
                null_skipped += 1
                continue

            try:
                was_inserted = transition_timezone_table.insert(TransitionTimezoneRecord(
                    transitioned_at=ts,
                    from_tz=current_tz,
                    to_tz=row_tz,
                    from_offset=current_offset,
                    to_offset=to_offset,
                ))

                if was_inserted:
                    inserted += 1
                    logger.info(
                        "Timezone transition at %s: %s → %s (%s)",
                        ts, current_tz or "none", row_tz, to_offset,
                    )
                else:
                    skipped += 1

            except Exception:
                logger.exception(
                    "Failed to insert timezone transition at %s (%s → %s)",
                    ts, current_tz, row_tz,
                )

            # Advance state regardless of whether INSERT succeeded —
            # we don't want to re-detect the same transition on every row.
            current_tz = row_tz
            current_offset = to_offset

        offset += PAGE_SIZE

    return {
        "inserted": inserted,
        "already_recorded": skipped,
        "null_tz_skipped": null_skipped,
    }


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run() -> dict:
    with get_conn() as conn:
        results = _detect_transitions(conn)

    logger.info(
        "timezone_transitions complete — inserted=%d, skipped=%d, null_tz=%d",
        results["inserted"],
        results["already_recorded"],
        results["null_tz_skipped"],
    )
    return results


if __name__ == "__main__":
    configure_logging()
    with CronJobMailer(
        "detect_timezone_transitions",
        settings.smtp_config,
        detail="Detect IANA timezone changes from location history",
    ) as job:
        results = run()
        job.add_metric("transitions inserted", results["inserted"])
        job.add_metric("already recorded (skipped)", results["already_recorded"])
        job.add_metric("unresolvable tz skipped", results["null_tz_skipped"])