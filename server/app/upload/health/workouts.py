import logging
from typing import Any

from database.health.workouts.table import table as workouts_table, WorkoutRecord, WorkoutRouteRecord
from upload.health.processing import parse_unix

logger = logging.getLogger(__name__)

# Unit conversion factors to SI base units
_DISTANCE_TO_M: dict[str, float] = {
    "m": 1.0,
    "km": 1000.0,
    "mi": 1609.344,
    "ft": 0.3048,
    "yd": 0.9144,
}
_SPEED_TO_MS: dict[str, float] = {
    "m/s": 1.0,
    "km/h": 1 / 3.6,
    "mph": 0.44704,
}


def _to_metres(value: float | None, units: str | None) -> float | None:
    """Convert a distance value to metres. Returns raw value if units unknown."""
    if value is None:
        return None
    factor = _DISTANCE_TO_M.get((units or "").lower().strip())
    return round(value * factor, 3) if factor else value


def _to_ms(value: float | None, units: str | None) -> float | None:
    """Convert a speed value to m/s. Returns raw value if units unknown."""
    if value is None:
        return None
    factor = _SPEED_TO_MS.get((units or "").lower().strip())
    return round(value * factor, 4) if factor else value


def _qty(obj: dict | None) -> float | None:
    """Safely extract the 'qty' field from a HAE quantity dict, or None."""
    if obj is None:
        return None
    return obj.get("qty")


def _units(obj: dict | None) -> str | None:
    """Safely extract the 'units' field from a HAE quantity dict, or None."""
    if obj is None:
        return None
    return obj.get("units")


def handle_workout_upload(data: dict[str, Any]):
    """Parse and insert all workouts (and their route points) from a HAE workout export."""
    logger.info("Processing workout data in the background...")

    workouts = data.get("workouts")
    if not workouts:
        logger.error("No 'workouts' key found in workout payload.")
        return

    inserted = 0
    skipped = 0
    route_points = 0

    for w in workouts:
        workout_id = w.get("id")
        name = w.get("name")
        start_str = w.get("start")
        end_str = w.get("end")
        duration = w.get("duration")

        if not all([workout_id, name, start_str, end_str, duration is not None]):
            logger.warning("Skipping workout missing required fields: %s", w)
            skipped += 1
            continue

        try:
            start_ts = parse_unix(start_str)
            end_ts = parse_unix(end_str)
        except (ValueError, TypeError) as e:
            logger.warning("Skipping workout '%s' - bad date: %s", workout_id, e)
            skipped += 1
            continue

        # Energy
        active_energy = _qty(w.get("activeEnergyBurned"))
        total_energy = _qty(w.get("totalEnergy"))

        # Distance — normalise to metres
        distance_obj = w.get("distance")
        distance_m = _to_metres(_qty(distance_obj), _units(distance_obj))

        # Speed — normalise to m/s
        avg_speed_obj = w.get("avgSpeed") or w.get("speed")
        avg_speed_ms = _to_ms(_qty(avg_speed_obj), _units(avg_speed_obj) or _units(w.get("speed")))
        max_speed_ms = _to_ms(_qty(w.get("maxSpeed")), _units(w.get("maxSpeed")))

        # Elevation — normalise to metres
        elev_up_obj = w.get("elevationUp")
        elev_down_obj = w.get("elevationDown")
        elevation_up_m = _to_metres(_qty(elev_up_obj), _units(elev_up_obj))
        elevation_down_m = _to_metres(_qty(elev_down_obj), _units(elev_down_obj))

        # Heart rate
        hr = w.get("heartRate") or {}
        hr_min = _qty(w.get("minHeartRate")) or _qty(hr.get("min"))
        hr_avg = _qty(w.get("avgHeartRate")) or _qty(hr.get("avg"))
        hr_max = _qty(w.get("maxHeartRate")) or _qty(hr.get("max"))

        # Intensity / cadence
        intensity_met = _qty(w.get("intensity"))
        step_cadence = _qty(w.get("stepCadence"))
        flights_climbed = _qty(w.get("flightsClimbed"))

        # Swimming-specific — lap_length normalised to metres
        lap_length_obj = w.get("lapLength")
        lap_length_m = _to_metres(_qty(lap_length_obj), _units(lap_length_obj))
        stroke_style = w.get("strokeStyle")
        swolf_score = w.get("swolfScore")
        salinity = w.get("salinity")
        swim_stroke_count = _qty(w.get("totalSwimmingStrokeCount"))
        swim_cadence = _qty(w.get("swimCadence"))

        is_indoor = w.get("isIndoor")

        was_inserted = workouts_table.insert(WorkoutRecord(
            id=workout_id,
            name=name,
            start_ts=start_ts,
            end_ts=end_ts,
            duration_s=int(duration),
            is_indoor=is_indoor,
            active_energy_kcal=active_energy,
            total_energy_kcal=total_energy,
            distance_m=distance_m,
            avg_speed_ms=avg_speed_ms,
            max_speed_ms=max_speed_ms,
            elevation_up_m=elevation_up_m,
            elevation_down_m=elevation_down_m,
            hr_min=hr_min,
            hr_avg=hr_avg,
            hr_max=hr_max,
            intensity_met=intensity_met,
            step_cadence=step_cadence,
            flights_climbed=flights_climbed,
            lap_length_m=lap_length_m,
            stroke_style=stroke_style,
            swolf_score=swolf_score,
            salinity=salinity,
            swim_stroke_count=swim_stroke_count,
            swim_cadence=swim_cadence,
        ))

        if not was_inserted:
            logger.info("Workout '%s' (%s) already exists, skipping.", workout_id, name)
            skipped += 1
            continue

        inserted += 1
        logger.info("Inserted workout '%s' (%s).", workout_id, name)

        # Route points - only insert if workout was new to avoid duplicates
        route = w.get("route", [])
        for point in route:
            ts_str = point.get("timestamp")
            lat = point.get("latitude")
            lon = point.get("longitude")

            if not all([ts_str, lat is not None, lon is not None]):
                continue

            try:
                ts = parse_unix(ts_str)
            except (ValueError, TypeError) as e:
                logger.warning("Skipping route point for workout '%s' - bad date: %s", workout_id, e)
                continue

            workouts_table.insert_route_point(WorkoutRouteRecord(
                workout_id=workout_id,
                timestamp=ts,
                latitude=lat,
                longitude=lon,
                altitude=point.get("altitude"),
                speed=point.get("speed"),
                speed_accuracy=point.get("speedAccuracy"),
                course=point.get("course"),
                course_accuracy=point.get("courseAccuracy"),
                horizontal_accuracy=point.get("horizontalAccuracy"),
                vertical_accuracy=point.get("verticalAccuracy"),
            ))
            route_points += 1

    logger.info(
        "Finished processing workouts: %d inserted, %d skipped, %d route points stored.",
        inserted,
        skipped,
        route_points,
    )
