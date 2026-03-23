"""
database/health/workouts/table.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Schema and insert helpers for the workouts and workout_route tables.

workouts stores one row per workout session with aggregated performance metrics.
workout_route stores individual GPS route points for workouts that include a
recorded route; each point FK-references its parent workout.
"""

from database.util import get_conn


def init() -> None:
    """Create the workouts and workout_route tables and their indexes if they do not exist."""
    with get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS workouts (
                id                  TEXT PRIMARY KEY,
                name                TEXT NOT NULL,
                start_ts            INTEGER NOT NULL,
                end_ts              INTEGER NOT NULL,
                duration            INTEGER NOT NULL,
                location            TEXT,
                is_indoor           INTEGER,
                active_energy_kcal  REAL,
                total_energy_kcal   REAL,
                distance            REAL,
                distance_units      TEXT,
                avg_speed           REAL,
                max_speed           REAL,
                speed_units         TEXT,
                elevation_up        REAL,
                elevation_down      REAL,
                elevation_units     TEXT,
                hr_min              REAL,
                hr_avg              REAL,
                hr_max              REAL,
                intensity_met       REAL,
                humidity            REAL,
                temperature         REAL,
                temperature_units   TEXT,
                step_cadence        REAL,
                flights_climbed     REAL,
                lap_length          REAL,
                lap_length_units    TEXT,
                stroke_style        TEXT,
                swolf_score         REAL,
                salinity            TEXT,
                swim_stroke_count   REAL,
                swim_cadence        REAL,
                created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS workout_route (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                workout_id          TEXT NOT NULL REFERENCES workouts(id),
                timestamp           INTEGER NOT NULL,
                latitude            REAL NOT NULL,
                longitude           REAL NOT NULL,
                altitude            REAL,
                speed               REAL,
                speed_accuracy      REAL,
                course              REAL,
                course_accuracy     REAL,
                horizontal_accuracy REAL,
                vertical_accuracy   REAL
            );
        """)

        # workouts indexes
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_workouts_start_ts
            ON workouts (start_ts);
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_workouts_name
            ON workouts (name);
        """)

        # workout_route indexes
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_workout_route_workout_id
            ON workout_route (workout_id);
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_workout_route_timestamp
            ON workout_route (timestamp);
        """)


def insert_workout(
    id: str,
    name: str,
    start_ts: int,
    end_ts: int,
    duration: int,
    location: str | None,
    is_indoor: bool | None,
    active_energy_kcal: float | None,
    total_energy_kcal: float | None,
    distance: float | None,
    distance_units: str | None,
    avg_speed: float | None,
    max_speed: float | None,
    speed_units: str | None,
    elevation_up: float | None,
    elevation_down: float | None,
    elevation_units: str | None,
    hr_min: float | None,
    hr_avg: float | None,
    hr_max: float | None,
    intensity_met: float | None,
    humidity: float | None,
    temperature: float | None,
    temperature_units: str | None,
    step_cadence: float | None,
    flights_climbed: float | None,
    lap_length: float | None,
    lap_length_units: str | None,
    stroke_style: str | None,
    swolf_score: float | None,
    salinity: str | None,
    swim_stroke_count: float | None,
    swim_cadence: float | None,
) -> bool:
    """Insert a workout row. Returns True if inserted, False if already existed."""
    with get_conn() as conn:
        cursor = conn.execute("""
            INSERT OR IGNORE INTO workouts (
                id, name, start_ts, end_ts, duration,
                location, is_indoor,
                active_energy_kcal, total_energy_kcal,
                distance, distance_units,
                avg_speed, max_speed, speed_units,
                elevation_up, elevation_down, elevation_units,
                hr_min, hr_avg, hr_max,
                intensity_met,
                humidity,
                temperature, temperature_units,
                step_cadence, flights_climbed,
                lap_length, lap_length_units,
                stroke_style, swolf_score, salinity,
                swim_stroke_count, swim_cadence
            ) VALUES (
                ?, ?, ?, ?, ?,
                ?, ?,
                ?, ?,
                ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?,
                ?,
                ?, ?,
                ?, ?,
                ?, ?,
                ?, ?, ?,
                ?, ?
            );
        """, (
            id, name, start_ts, end_ts, duration,
            location, int(is_indoor) if is_indoor is not None else None,
            active_energy_kcal, total_energy_kcal,
            distance, distance_units,
            avg_speed, max_speed, speed_units,
            elevation_up, elevation_down, elevation_units,
            hr_min, hr_avg, hr_max,
            intensity_met,
            humidity,
            temperature, temperature_units,
            step_cadence, flights_climbed,
            lap_length, lap_length_units,
            stroke_style, swolf_score, salinity,
            swim_stroke_count, swim_cadence,
        ))
        return cursor.rowcount > 0


def insert_workout_route_point(
    workout_id: str,
    timestamp: int,
    latitude: float,
    longitude: float,
    altitude: float | None,
    speed: float | None,
    speed_accuracy: float | None,
    course: float | None,
    course_accuracy: float | None,
    horizontal_accuracy: float | None,
    vertical_accuracy: float | None,
):
    """Insert a single GPS route point for a workout.

    Route points are only stored for newly inserted workouts (checked in
    handle_workout_upload) to avoid duplicate points on re-upload.
    """
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO workout_route (
                workout_id, timestamp, latitude, longitude,
                altitude, speed, speed_accuracy,
                course, course_accuracy,
                horizontal_accuracy, vertical_accuracy
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """, (
            workout_id, timestamp, latitude, longitude,
            altitude, speed, speed_accuracy,
            course, course_accuracy,
            horizontal_accuracy, vertical_accuracy,
        ))