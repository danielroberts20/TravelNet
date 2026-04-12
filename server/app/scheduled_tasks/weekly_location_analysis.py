"""
scheduled_tasks/weekly_location_analysis.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Weekly Sunday pipeline that geocodes any new places then runs all three
location-analysis passes in dependency order:

  1. geocode_places              — populates places.country_code / places.timezone
  2. detect_timezone_transitions — depends on places.timezone
  3. detect_country_transitions  — depends on places.country_code
  4. detect_flights              — independent, but part of the same weekly pass

geocode_places also runs as a standalone daily deployment; the call here is
idempotent (places geocoded earlier that day are skipped).
"""
from config.editable import load_overrides
load_overrides()

from prefect import task, flow
from prefect.logging import get_run_logger

from notifications import notify_on_completion
from database.connection import get_conn
from scheduled_tasks.geocode_places import geocode_places_flow
from scheduled_tasks.detect_timezone_transitions import detect_timezone_transitions_flow
from scheduled_tasks.detect_country_transitions import detect_country_transitions_flow
from scheduled_tasks.detect_flights import detect_flights_flow
from scheduled_tasks.update_timezone import update_timezones_flow


@task
def get_current_timezone() -> str | None:
    """Return the to_tz of the most recent row in timezone_transitions, or None if empty."""
    logger = get_run_logger()
    with get_conn(read_only=True) as conn:
        row = conn.execute("""
            SELECT to_tz FROM transition_timezone
            ORDER BY transitioned_at DESC
            LIMIT 1
        """).fetchone()
    if row:
        logger.info("Current timezone: %s", row["to_tz"])
        return row["to_tz"]
    logger.info("No timezone transitions recorded yet — skipping deployment schedule update")
    return None


@flow(name="Weekly Location Analysis", on_completion=[notify_on_completion], on_failure=[notify_on_completion])
def weekly_location_analysis_flow():
    logger = get_run_logger()
    logger.info("Starting weekly location analysis pipeline")

    geocode_result = geocode_places_flow()
    logger.info("geocode_places complete: %s", geocode_result)

    tz_result = detect_timezone_transitions_flow()
    logger.info("detect_timezone_transitions complete: %s", tz_result)

    country_result = detect_country_transitions_flow()
    logger.info("detect_country_transitions complete: %s", country_result)

    flight_result = detect_flights_flow()
    logger.info("detect_flights complete: %s", flight_result)

    current_tz = get_current_timezone()
    if current_tz:
        update_timezones_flow(timezone=current_tz)
        logger.info("Deployment schedules updated to %s", current_tz)

    return {
        "geocoded": geocode_result,
        "timezone_transitions": tz_result,
        "country_transitions": country_result,
        "flights": flight_result,
        "timezone_updated_to": current_tz,
    }
