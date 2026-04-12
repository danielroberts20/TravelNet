"""
scheduled_tasks/detect_country_transitions.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Walks location_unified (joined to places) in chronological order and records
country transitions — moments when the user crosses into a new country.

A dwell filter (DWELL_MIN_POINTS consecutive points) prevents brief GPS
noise or miscoded border points from creating false transitions.

Safe to re-run — INSERT OR IGNORE on (country_code, entered_at). The
departed_at UPDATE is also idempotent (no-op if already set).
"""
from config.editable import load_overrides
load_overrides()

from prefect import task, flow
from prefect.logging import get_run_logger

from config.general import DWELL_MIN_POINTS, PAGE_SIZE
from database.transition.country.table import table as country_transition_table, CountryTransitionRecord
from database.connection import get_conn


def _detect_transitions(conn, logger) -> dict:
    """
    Walk location_unified chronologically and detect country changes.

    State machine:
      confirmed_country   — country we've accepted as current
      candidate_country   — new country we're evaluating
      candidate_count     — consecutive points seen in candidate
      candidate_*         — metadata for the first point in candidate
                            (used as entered_at if confirmed)

    Only promotes candidate → confirmed after DWELL_MIN_POINTS consecutive
    points, filtering out brief GPS miscodes or border-skimming routes.
    """
    inserted = 0
    updated = 0    # departed_at fills
    skipped = 0    # INSERT OR IGNORE no-ops

    # Confirmed state
    confirmed_country_code: str | None = None
    confirmed_country_name: str | None = None

    # Candidate (potential new country, not yet confirmed)
    candidate_code: str | None = None
    candidate_name: str | None = None
    candidate_count: int = 0
    candidate_first_ts: str | None = None
    candidate_first_lat: float | None = None
    candidate_first_lon: float | None = None
    candidate_first_place_id: int | None = None

    def reset_candidate() -> None:
        nonlocal candidate_code, candidate_name, candidate_count
        nonlocal candidate_first_ts, candidate_first_lat
        nonlocal candidate_first_lon, candidate_first_place_id
        candidate_code = None
        candidate_name = None
        candidate_count = 0
        candidate_first_ts = None
        candidate_first_lat = None
        candidate_first_lon = None
        candidate_first_place_id = None

    def confirm_transition() -> None:
        """Promote the current candidate to confirmed country."""
        nonlocal confirmed_country_code, confirmed_country_name
        nonlocal inserted, updated, skipped

        # Record departure from previous country
        if confirmed_country_code is not None:
            country_transition_table.update_departed_at(
                confirmed_country_code,
                candidate_first_ts,  # departed at the first point in new country
            )
            updated += 1
            logger.info(
                "Departed %s at %s",
                confirmed_country_code, candidate_first_ts,
            )

        # Insert entry into new country
        was_inserted = country_transition_table.insert(CountryTransitionRecord(
            country_code=candidate_code,
            country=candidate_name,
            entered_at=candidate_first_ts,
            entry_lat=candidate_first_lat,
            entry_lon=candidate_first_lon,
            entry_place_id=candidate_first_place_id,
        ))

        if was_inserted:
            inserted += 1
            logger.info(
                "Entered %s (%s) at %s",
                candidate_name, candidate_code, candidate_first_ts,
            )
        else:
            skipped += 1

        confirmed_country_code = candidate_code
        confirmed_country_name = candidate_name

    offset = 0

    while True:
        rows = conn.execute("""
            SELECT
                u.timestamp,
                u.latitude,
                u.longitude,
                p.country_code,
                p.country,
                p.id AS place_id
            FROM location_unified u
            JOIN places p ON u.place_id = p.id
            WHERE p.country_code IS NOT NULL
              AND p.country_code != ''
              AND p.country IS NOT NULL
            ORDER BY u.timestamp ASC
            LIMIT ? OFFSET ?
        """, (PAGE_SIZE, offset)).fetchall()

        if not rows:
            break

        for row in rows:
            row_code = row["country_code"]
            row_name = row["country"]

            # Still in confirmed country — reset any pending candidate.
            if row_code == confirmed_country_code:
                reset_candidate()
                continue

            # Point is in a different country from confirmed.
            if row_code == candidate_code:
                # Continuing to accumulate points in the same candidate.
                candidate_count += 1
                if candidate_count >= DWELL_MIN_POINTS:
                    confirm_transition()
                    reset_candidate()
            else:
                # New candidate country (different from both confirmed and
                # previous candidate — e.g. brief third-country crossing).
                candidate_code = row_code
                candidate_name = row_name
                candidate_count = 1
                candidate_first_ts = row["timestamp"]
                candidate_first_lat = row["latitude"]
                candidate_first_lon = row["longitude"]
                candidate_first_place_id = row["place_id"]

        offset += PAGE_SIZE

    return {
        "inserted": inserted,
        "departed_at_updated": updated,
        "already_recorded": skipped,
    }


@task
def run_country_transition_detection() -> dict:
    logger = get_run_logger()
    with get_conn() as conn:
        results = _detect_transitions(conn, logger)

    logger.info(
        "country_transitions complete — inserted=%d, departures_updated=%d, skipped=%d",
        results["inserted"],
        results["departed_at_updated"],
        results["already_recorded"],
    )
    return results


@flow(name="Detect Country Transitions")
def detect_country_transitions_flow():
    return run_country_transition_detection()
