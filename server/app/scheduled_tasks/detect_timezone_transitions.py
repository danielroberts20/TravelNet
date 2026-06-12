"""
scheduled_tasks/detect_timezone_transitions.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Scans location_unified (joined to places) in chronological order and records
rows into timezone_transitions whenever the IANA timezone changes.

Safe to re-run — uses INSERT OR IGNORE on (transitioned_at, to_tz).
"""
from config.editable import load_overrides
load_overrides()

from datetime import datetime, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from prefect import task, flow
from prefect.logging import get_run_logger

from database.transition.timezone.table import table as transition_timezone_table, TransitionTimezoneRecord
from config.general import DWELL_MIN_POINTS, PAGE_SIZE
from database.connection import get_conn
from notifications import record_flow_result, notify_on_completion, log_on_success


def _utc_offset_str(iana_tz: str, at_utc_iso: str, logger) -> str | None:
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


def _detect_transitions(conn, logger) -> dict:
    """
    Walk location_unified joined to places in chronological order.
    Insert a timezone_transitions row each time places.timezone changes.

    State machine:
      current_tz / current_offset — timezone we've accepted as current
      candidate_tz / candidate_offset — new timezone we're evaluating
      candidate_count              — consecutive points seen in candidate
      candidate_first_ts / candidate_first_place_id
                                   — metadata for the first point in candidate
                                     (used as transitioned_at if confirmed)

    Only promotes candidate → current after DWELL_MIN_POINTS consecutive
    points, filtering out brief GPS noise at landing or border areas.

    Returns a dict with diagnostic counts.
    """
    inserted = 0
    skipped = 0       # INSERT OR IGNORE hits (already recorded)
    null_skipped = 0  # rows where place has no timezone yet
    dwell_suppressed = 0  # candidates reset before reaching DWELL_MIN_POINTS

    # Confirmed state
    current_tz: str | None = None
    current_offset: str | None = None

    # Candidate (potential new timezone, not yet confirmed)
    candidate_tz: str | None = None
    candidate_offset: str | None = None
    candidate_count: int = 0
    candidate_first_ts: str | None = None
    candidate_first_place_id: int | None = None

    def reset_candidate() -> None:
        nonlocal candidate_tz, candidate_offset, candidate_count
        nonlocal candidate_first_ts, candidate_first_place_id
        candidate_tz = None
        candidate_offset = None
        candidate_count = 0
        candidate_first_ts = None
        candidate_first_place_id = None

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

            # Still in confirmed timezone — reset any pending candidate.
            if row_tz == current_tz:
                if candidate_count > 0:
                    dwell_suppressed += 1
                reset_candidate()
                continue

            # Point is in a different timezone from confirmed.
            if row_tz == candidate_tz:
                # Continuing to accumulate points in the same candidate.
                candidate_count += 1
                if candidate_count >= DWELL_MIN_POINTS:
                    ts = candidate_first_ts
                    to_offset = candidate_offset

                    try:
                        was_inserted = transition_timezone_table.insert(TransitionTimezoneRecord(
                            transitioned_at=ts,
                            from_tz=current_tz,
                            to_tz=candidate_tz,
                            from_offset=current_offset,
                            to_offset=to_offset,
                        ))

                        if was_inserted:
                            inserted += 1
                            logger.info(
                                "Timezone transition at %s: %s → %s (%s)",
                                ts, current_tz or "none", candidate_tz, to_offset,
                            )
                        else:
                            skipped += 1

                    except Exception:
                        logger.exception(
                            "Failed to insert timezone transition at %s (%s → %s)",
                            ts, current_tz, candidate_tz,
                        )

                    # Advance state regardless of whether INSERT succeeded —
                    # we don't want to re-detect the same transition on every row.
                    current_tz = candidate_tz
                    current_offset = candidate_offset
                    reset_candidate()
            else:
                # New candidate timezone (different from both confirmed and
                # previous candidate — e.g. brief third-zone noise crossing).
                if candidate_count > 0:
                    dwell_suppressed += 1
                ts = row["timestamp"]
                to_offset = _utc_offset_str(row_tz, ts, logger)
                if to_offset is None:
                    # Unresolvable timezone — skip without updating candidate so
                    # we don't lose track of the last known good timezone.
                    null_skipped += 1
                else:
                    candidate_tz = row_tz
                    candidate_offset = to_offset
                    candidate_count = 1
                    candidate_first_ts = ts
                    candidate_first_place_id = row["place_id"]

        offset += PAGE_SIZE

    return {
        "inserted": inserted,
        "already_recorded": skipped,
        "null_tz_skipped": null_skipped,
        "dwell_suppressed": dwell_suppressed,
    }


@task
def run_timezone_transition_detection() -> dict:
    logger = get_run_logger()
    with get_conn() as conn:
        results = _detect_transitions(conn, logger)

    logger.info(
        "timezone_transitions complete — inserted=%d, skipped=%d, null_tz=%d, dwell_suppressed=%d",
        results["inserted"],
        results["already_recorded"],
        results["null_tz_skipped"],
        results["dwell_suppressed"],
    )
    return results


@flow(name="Detect Timezone Transitions", on_failure=[notify_on_completion], on_completion=[log_on_success])
def detect_timezone_transitions_flow():
    result = run_timezone_transition_detection()
    record_flow_result(result)
    return result
