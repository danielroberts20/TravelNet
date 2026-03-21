from datetime import datetime
import logging
from typing import Any

from database.health.workouts.table import insert_workout, insert_workout_route_point

logger = logging.getLogger(__name__)


def parse_unix(date_str: str) -> int:
    return int(datetime.strptime(date_str.strip(), "%Y-%m-%d %H:%M:%S %z").timestamp())


def _qty(obj: dict | None) -> float | None:
    if obj is None:
        return None
    return obj.get("qty")


def _units(obj: dict | None) -> str | None:
    if obj is None:
        return None
    return obj.get("units")


def handle_workout_upload(data: dict[str, Any]):
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

        # Distance
        distance_obj = w.get("distance")
        distance = _qty(distance_obj)
        distance_units = _units(distance_obj)

        # Speed
        avg_speed_obj = w.get("avgSpeed") or w.get("speed")
        avg_speed = _qty(avg_speed_obj)
        max_speed = _qty(w.get("maxSpeed"))
        speed_units = _units(avg_speed_obj) or _units(w.get("maxSpeed"))

        # Elevation
        elev_up_obj = w.get("elevationUp")
        elev_down_obj = w.get("elevationDown")
        elevation_up = _qty(elev_up_obj)
        elevation_down = _qty(elev_down_obj)
        elevation_units = _units(elev_up_obj) or _units(elev_down_obj)

        # Heart rate -- HAE sends flat minHeartRate/avgHeartRate/maxHeartRate fields,
        # falling back to nested heartRate.min/avg/max if present
        hr = w.get("heartRate") or {}
        hr_min = _qty(w.get("minHeartRate")) or _qty(hr.get("min"))
        hr_avg = _qty(w.get("avgHeartRate")) or _qty(hr.get("avg"))
        hr_max = _qty(w.get("maxHeartRate")) or _qty(hr.get("max"))

        # Intensity / environment
        intensity_met = _qty(w.get("intensity"))
        humidity = _qty(w.get("humidity"))
        temp_obj = w.get("temperature")
        temperature = _qty(temp_obj)
        temperature_units = _units(temp_obj)

        # Step / cadence
        step_cadence = _qty(w.get("stepCadence"))
        flights_climbed = _qty(w.get("flightsClimbed"))

        # Swimming-specific
        lap_length_obj = w.get("lapLength")
        lap_length = _qty(lap_length_obj)
        lap_length_units = _units(lap_length_obj)
        stroke_style = w.get("strokeStyle")
        swolf_score = w.get("swolfScore")
        salinity = w.get("salinity")
        swim_stroke_count = _qty(w.get("totalSwimmingStrokeCount"))
        swim_cadence = _qty(w.get("swimCadence"))

        # General
        location = w.get("location")
        is_indoor = w.get("isIndoor")

        was_inserted = insert_workout(
            id=workout_id,
            name=name,
            start_ts=start_ts,
            end_ts=end_ts,
            duration=int(duration),
            location=location,
            is_indoor=is_indoor,
            active_energy_kcal=active_energy,
            total_energy_kcal=total_energy,
            distance=distance,
            distance_units=distance_units,
            avg_speed=avg_speed,
            max_speed=max_speed,
            speed_units=speed_units,
            elevation_up=elevation_up,
            elevation_down=elevation_down,
            elevation_units=elevation_units,
            hr_min=hr_min,
            hr_avg=hr_avg,
            hr_max=hr_max,
            intensity_met=intensity_met,
            humidity=humidity,
            temperature=temperature,
            temperature_units=temperature_units,
            step_cadence=step_cadence,
            flights_climbed=flights_climbed,
            lap_length=lap_length,
            lap_length_units=lap_length_units,
            stroke_style=stroke_style,
            swolf_score=swolf_score,
            salinity=salinity,
            swim_stroke_count=swim_stroke_count,
            swim_cadence=swim_cadence,
        )

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

            insert_workout_route_point(
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
            )
            route_points += 1

    logger.info(
        "Finished processing workouts: %d inserted, %d skipped, %d route points stored.",
        inserted,
        skipped,
        route_points,
    )