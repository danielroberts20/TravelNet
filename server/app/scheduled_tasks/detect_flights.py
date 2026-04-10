"""
scheduled_tasks/detect_flights.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Scans location_unified for large gaps that look like flights and inserts
draft rows into the flights table with source='detected'.
 
Criteria for a flight gap:
  - Time gap > FLIGHT_GAP_MIN_HOURS between consecutive location points
  - Great-circle distance between last point before gap and first point
    after gap > FLIGHT_DISTANCE_MIN_KM
 
Detected rows are drafts — confirm or delete them via the Dashboard.
Already-confirmed manual rows are not overwritten (INSERT OR IGNORE on
the same UNIQUE key would fire, but detected rows won't match manual ones
unless the timestamps happen to match exactly).
 
Run via:
    cd server && ./runcron.sh scheduled_tasks.detect_flights
"""
 
import logging
from datetime import datetime, timezone as dt_timezone
 
from util import haversine_km
from config.logging import configure_logging
from config.general import FLIGHT_GAP_MIN_HOURS, FLIGHT_DISTANCE_MIN_KM
from database.connection import get_conn
from database.flights.table import table as flights_table, FlightRecord
from notifications import CronJobMailer
from config.settings import settings
 
logger = logging.getLogger(__name__)
 
 
def _ts_to_dt(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(dt_timezone.utc)
 
 
def _detect_flight_gaps(conn) -> list[dict]:
    """
    Fetch all location points ordered by timestamp and find consecutive pairs
    where the gap meets the flight criteria.
 
    Returns a list of dicts describing each candidate gap.
    """
    # Pull only what we need: timestamp + coordinates.
    # Use location_overland_cleaned to avoid noise points inflating gaps.
    rows = conn.execute("""
        SELECT timestamp, latitude, longitude
        FROM location_overland_cleaned
        ORDER BY timestamp ASC
    """).fetchall()
 
    if len(rows) < 2:
        return []
 
    candidates = []
    for prev, curr in zip(rows, rows[1:]):
        prev_dt = _ts_to_dt(prev["timestamp"])
        curr_dt = _ts_to_dt(curr["timestamp"])
        gap_hours = (curr_dt - prev_dt).total_seconds() / 3600
 
        if gap_hours < FLIGHT_GAP_MIN_HOURS:
            continue
 
        dist_km = haversine_km(
            prev["latitude"], prev["longitude"],
            curr["latitude"], curr["longitude"],
        )
 
        if dist_km < FLIGHT_DISTANCE_MIN_KM:
            continue
 
        candidates.append({
            "gap_start_ts": prev["timestamp"],
            "gap_end_ts": curr["timestamp"],
            "gap_start_lat": prev["latitude"],
            "gap_start_lon": prev["longitude"],
            "gap_end_lat": curr["latitude"],
            "gap_end_lon": curr["longitude"],
            "gap_hours": round(gap_hours, 1),
            "distance_km": round(dist_km, 1),
        })
 
    return candidates
 
 
def run() -> dict:
    with get_conn() as conn:
        candidates = _detect_flight_gaps(conn)
 
    if not candidates:
        logger.info("detect_flights: no new flight gaps found")
        return {"detected": 0, "inserted": 0}
 
    inserted = 0
    for gap in candidates:
        # Use gap timestamps as departed_at/arrived_at — best approximation
        # without knowing actual flight schedules.
        duration_mins = round(gap["gap_hours"] * 60)
 
        record = FlightRecord(
            origin_iata="???",              # unknown — user must fill in
            destination_iata="???",
            departed_at=gap["gap_start_ts"],
            arrived_at=gap["gap_end_ts"],
            duration_mins=duration_mins,
            distance_km=gap["distance_km"],
            notes=(
                f"Auto-detected gap: {gap['gap_hours']}h, {gap['distance_km']}km. "
                f"Origin approx ({gap['gap_start_lat']:.3f}, {gap['gap_start_lon']:.3f}), "
                f"destination approx ({gap['gap_end_lat']:.3f}, {gap['gap_end_lon']:.3f}). "
                f"Please confirm and fill in IATA codes."
            ),
            source="detected",
        )
 
        # UNIQUE is on (origin_iata, destination_iata, departed_at).
        # Detected rows use "???" for both IATA fields, so they can clash
        # if the same gap is detected twice.  INSERT OR IGNORE handles this.
        was_inserted = flights_table.insert(record)
        if was_inserted:
            inserted += 1
            logger.info(
                "Flight gap detected: %s → %s (%.0fh, %.0fkm)",
                gap["gap_start_ts"], gap["gap_end_ts"],
                gap["gap_hours"], gap["distance_km"],
            )
 
    logger.info("detect_flights: %d gaps found, %d inserted", len(candidates), inserted)
    return {"detected": len(candidates), "inserted": inserted}
 
 
if __name__ == "__main__":
    configure_logging()
    with CronJobMailer(
        "detect_flights",
        settings.smtp_config,
        detail="Detect flight gaps from location history",
    ) as job:
        results = run()
        job.add_metric("gaps detected", results["detected"])
        job.add_metric("draft rows inserted", results["inserted"])