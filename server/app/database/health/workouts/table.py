"""
database/health/workouts/table.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Schema and insert helpers for the workouts and workout_route tables.

workouts stores one row per workout session with aggregated performance metrics.
workout_route stores individual GPS route points for workouts that include a
recorded route; each route point FK-references its parent workout.

Two table classes share this module because the route table only exists in the
context of workouts and is always initialised together with it.
"""

from dataclasses import dataclass

from database.base import BaseTable
from database.connection import get_conn, to_iso_str


@dataclass
class WorkoutRecord:
    id: str
    name: str
    start_ts: int
    end_ts: int
    duration_s: int
    is_indoor: bool | None = None
    active_energy_kcal: float | None = None
    total_energy_kcal: float | None = None
    distance_m: float | None = None
    avg_speed_ms: float | None = None
    max_speed_ms: float | None = None
    elevation_up_m: float | None = None
    elevation_down_m: float | None = None
    hr_min: float | None = None
    hr_avg: float | None = None
    hr_max: float | None = None
    intensity_met: float | None = None
    step_cadence: float | None = None
    flights_climbed: float | None = None
    lap_length_m: float | None = None
    stroke_style: str | None = None
    swolf_score: float | None = None
    salinity: str | None = None
    swim_stroke_count: float | None = None
    swim_cadence: float | None = None


@dataclass
class WorkoutRouteRecord:
    workout_id: str
    timestamp: int
    latitude: float
    longitude: float
    altitude: float | None = None
    speed: float | None = None
    speed_accuracy: float | None = None
    course: float | None = None
    course_accuracy: float | None = None
    horizontal_accuracy: float | None = None
    vertical_accuracy: float | None = None


class WorkoutsTable(BaseTable[WorkoutRecord]):

    def init(self) -> None:
        """Create the workouts and workout_route tables and their indexes."""
        with get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS workouts (
                    id                  TEXT PRIMARY KEY,
                    name                TEXT NOT NULL,
                    start_ts            TEXT NOT NULL,
                    end_ts              TEXT NOT NULL,
                    duration_s          INTEGER NOT NULL,
                    is_indoor           INTEGER,
                    active_energy_kcal  REAL,
                    total_energy_kcal   REAL,
                    distance_m          REAL,
                    avg_speed_ms        REAL,
                    max_speed_ms        REAL,
                    elevation_up_m      REAL,
                    elevation_down_m    REAL,
                    hr_min              REAL,
                    hr_avg              REAL,
                    hr_max              REAL,
                    intensity_met       REAL,
                    step_cadence        REAL,
                    flights_climbed     REAL,
                    lap_length_m        REAL,
                    stroke_style        TEXT,
                    swolf_score         REAL,
                    salinity            TEXT,
                    swim_stroke_count   REAL,
                    swim_cadence        REAL,
                    start_place_id      INTEGER REFERENCES places(id),
                    created_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
                );
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS workout_route (
                    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                    workout_id          TEXT NOT NULL REFERENCES workouts(id),
                    timestamp           TEXT NOT NULL,
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

            conn.execute("CREATE INDEX IF NOT EXISTS idx_workouts_start_ts ON workouts (start_ts);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_workouts_name ON workouts (name);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_workout_route_workout_id ON workout_route (workout_id);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_workout_route_timestamp ON workout_route (timestamp);")

    def insert(self, record: WorkoutRecord) -> bool:
        """Insert a workout row. Returns True if inserted, False if already existed."""
        start = to_iso_str(record.start_ts)
        end = to_iso_str(record.end_ts)
        with get_conn() as conn:
            cursor = conn.execute("""
                INSERT OR IGNORE INTO workouts (
                    id, name, start_ts, end_ts, duration_s,
                    is_indoor,
                    active_energy_kcal, total_energy_kcal,
                    distance_m, avg_speed_ms, max_speed_ms,
                    elevation_up_m, elevation_down_m,
                    hr_min, hr_avg, hr_max, intensity_met,
                    step_cadence, flights_climbed, lap_length_m,
                    stroke_style, swolf_score, salinity,
                    swim_stroke_count, swim_cadence
                ) VALUES (
                    ?, ?, ?, ?, ?,
                    ?,
                    ?, ?,
                    ?, ?, ?,
                    ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?,
                    ?, ?
                );
            """, (
                record.id, record.name, start, end, record.duration_s,
                int(record.is_indoor) if record.is_indoor is not None else None,
                record.active_energy_kcal, record.total_energy_kcal,
                record.distance_m, record.avg_speed_ms, record.max_speed_ms,
                record.elevation_up_m, record.elevation_down_m,
                record.hr_min, record.hr_avg, record.hr_max, record.intensity_met,
                record.step_cadence, record.flights_climbed, record.lap_length_m,
                record.stroke_style, record.swolf_score, record.salinity,
                record.swim_stroke_count, record.swim_cadence,
            ))
            return cursor.rowcount > 0

    def insert_route_point(self, record: WorkoutRouteRecord) -> None:
        """Insert a single GPS route point for a workout."""
        ts = to_iso_str(record.timestamp)
        with get_conn() as conn:
            conn.execute("""
                INSERT INTO workout_route (
                    workout_id, timestamp, latitude, longitude,
                    altitude, speed, speed_accuracy,
                    course, course_accuracy,
                    horizontal_accuracy, vertical_accuracy
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """, (
                record.workout_id, ts, record.latitude, record.longitude,
                record.altitude, record.speed, record.speed_accuracy,
                record.course, record.course_accuracy,
                record.horizontal_accuracy, record.vertical_accuracy,
            ))


table = WorkoutsTable()
