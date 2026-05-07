"""
database/summary/table.py
~~~~~~~~~~~~~~~~~~~~~~~~~

DailySummaryTable — owned/updated via the Domain abstraction in
scheduled_tasks.daily_summary.base. Each subflow writes only its own
columns via INSERT ... ON CONFLICT(date) DO UPDATE SET ...

Note: individual subflows do NOT go through this class's methods for
writes — they use `_upsert_domain_columns` in base.py. This class is
kept for `init()` (schema creation) and future explicit full-row ops.
"""
from dataclasses import dataclass
from typing import Optional

from database.base import BaseTable
from database.connection import get_conn


@dataclass
class DailySummaryRecord:
    """
    Full row shape. Domain subflows construct partial dicts with only
    the columns they own; this dataclass exists mainly as a reference
    and for typed one-shot operations.
    """
    date:       str
    timezone:   Optional[str] = None
    utc_offset: Optional[str] = None
    utc_start:  Optional[str] = None
    utc_end:    Optional[str] = None
    day_label:  Optional[str] = None

    # Location
    movement_entropy: Optional[float] = None
    settling_day:     Optional[int]   = None
    novelty_score:    Optional[float] = None

    country_code:            Optional[str]   = None
    country:                 Optional[str]   = None
    region:                  Optional[str]   = None
    city:                    Optional[str]   = None
    dominant_place_id:       Optional[int]   = None
    dominant_known_place_id: Optional[int]   = None
    location_points:         Optional[int]   = None
    overland_points:         Optional[int]   = None
    overland_coverage_pct:   Optional[float] = None
    distinct_places:         Optional[int]   = None
    new_places_visited:      Optional[int]   = None
    was_in_transit:          Optional[int]   = None

    # Health — activity
    steps:                 Optional[int]   = None
    active_energy_kcal:    Optional[float] = None
    resting_energy_kcal:   Optional[float] = None
    distance_km:           Optional[float] = None
    flights_climbed:       Optional[int]   = None
    time_in_daylight_min:  Optional[int]   = None
    avg_walking_speed_kmh: Optional[float] = None
    vo2_max:               Optional[float] = None

    # Health — vitals
    resting_hr:       Optional[float] = None
    avg_hrv_ms:       Optional[float] = None
    avg_spo2_pct:     Optional[float] = None
    respiratory_rate: Optional[float] = None
    wrist_temp_c:     Optional[float] = None

    # Sleep
    wake_time_local:       Optional[str]   = None
    sleep_time_local:      Optional[str]   = None
    sleep_hours:           Optional[float] = None
    awake_hours:           Optional[float] = None
    deep_sleep_hours:      Optional[float] = None
    rem_sleep_hours:       Optional[float] = None
    light_sleep_hours:     Optional[float] = None
    sleep_efficiency_pct:  Optional[float] = None
    restorative_sleep_pct: Optional[float] = None

    # Training load
    workout_tss: Optional[float] = None
    atl:         Optional[float] = None
    ctl:         Optional[float] = None
    tsb:         Optional[float] = None

    # Sleep midpoint
    sleep_midpoint_hr: Optional[float] = None

    # Mood
    avg_valence:         Optional[float] = None
    mood_entries:        Optional[int]   = None
    mood_classification: Optional[str]  = None

    # Spending
    spend_gbp:         Optional[float] = None
    spend_local:       Optional[float] = None
    spend_currency:    Optional[str]   = None
    transaction_count: Optional[int]   = None
    spend_normalised:  Optional[float] = None

    # Weather
    temp_max_c:       Optional[float] = None
    temp_min_c:       Optional[float] = None
    precipitation_mm: Optional[float] = None
    weathercode:      Optional[int]   = None
    uv_index_max:     Optional[float] = None

    # Pi
    photo_count:                   Optional[int]   = None
    watchdog_heartbeats_received:  Optional[int]   = None
    watchdog_max_gap_mins:         Optional[int]   = None
    watchdog_max_consecutive_fail: Optional[int]   = None
    travelnet_internet_ok_pct:     Optional[float] = None
    travelnet_api_ok_pct:          Optional[float] = None
    prefect_ok_pct:                Optional[float] = None
    avg_w_pi:                      Optional[float] = None
    total_wh_pi:                   Optional[float] = None

    # ML outputs (schema-only — no domain owns these yet)
    anomaly_score:    Optional[float] = None
    is_anomaly:       int             = 0
    travel_phase:     Optional[str]   = None
    day_embedding_id: Optional[int]   = None

    # Completeness flags — one per domain
    health_complete:   int = 0
    location_complete: int = 0
    pi_complete:       int = 0
    spend_complete:    int = 0
    weather_complete:  int = 0


class DailySummaryTable(BaseTable[DailySummaryRecord]):

    def init(self) -> None:
        with get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS daily_summary (
                    id                            INTEGER PRIMARY KEY AUTOINCREMENT,

                    date                          TEXT NOT NULL UNIQUE,
                    timezone                      TEXT,
                    utc_offset                    TEXT,
                    utc_start                     TEXT,
                    utc_end                       TEXT,
                    day_label                     TEXT,

                    movement_entropy              REAL,
                    settling_day                  INTEGER,
                    novelty_score                 REAL,

                    country_code                  TEXT,
                    country                       TEXT,
                    region                        TEXT,
                    city                          TEXT,
                    dominant_place_id             INTEGER REFERENCES places(id),
                    dominant_known_place_id       INTEGER REFERENCES known_places(id),
                    location_points               INTEGER,
                    overland_points               INTEGER,
                    overland_coverage_pct         REAL,
                    distinct_places               INTEGER,
                    new_places_visited            INTEGER,
                    was_in_transit                INTEGER DEFAULT 0,

                    steps                         INTEGER,
                    active_energy_kcal            REAL,
                    resting_energy_kcal           REAL,
                    distance_km                   REAL,
                    flights_climbed               INTEGER,
                    time_in_daylight_min          INTEGER,
                    avg_walking_speed_kmh         REAL,
                    vo2_max                       REAL,

                    resting_hr                    REAL,
                    avg_hrv_ms                    REAL,
                    avg_spo2_pct                  REAL,
                    respiratory_rate              REAL,
                    wrist_temp_c                  REAL,

                    wake_time_local               TEXT,
                    sleep_time_local              TEXT,
                    sleep_hours                   REAL,
                    awake_hours                   REAL,
                    deep_sleep_hours              REAL,
                    rem_sleep_hours               REAL,
                    light_sleep_hours             REAL,
                    sleep_efficiency_pct          REAL,
                    restorative_sleep_pct         REAL,

                    workout_tss                   REAL,
                    atl                           REAL,
                    ctl                           REAL,
                    tsb                           REAL,

                    sleep_midpoint_hr             REAL,

                    avg_valence                   REAL,
                    mood_entries                  INTEGER,
                    mood_classification           TEXT,

                    spend_gbp                     REAL,
                    spend_local                   REAL,
                    spend_currency                TEXT,
                    transaction_count             INTEGER,
                    spend_normalised              REAL,

                    temp_max_c                    REAL,
                    temp_min_c                    REAL,
                    precipitation_mm              REAL,
                    weathercode                   INTEGER,
                    uv_index_max                  REAL,

                    photo_count                   INTEGER,
                    watchdog_heartbeats_received  INTEGER,
                    watchdog_max_gap_mins         INTEGER,
                    watchdog_max_consecutive_fail INTEGER,
                    travelnet_internet_ok_pct     REAL,
                    travelnet_api_ok_pct          REAL,
                    prefect_ok_pct                REAL,
                    avg_w_pi                      REAL,
                    total_wh_pi                   REAL,

                    anomaly_score                 REAL,
                    is_anomaly                    INTEGER DEFAULT 0,
                    travel_phase                  TEXT,
                    day_embedding_id              INTEGER,

                    health_complete               INTEGER NOT NULL DEFAULT 0,
                    location_complete             INTEGER NOT NULL DEFAULT 0,
                    pi_complete                   INTEGER NOT NULL DEFAULT 0,
                    spend_complete                INTEGER NOT NULL DEFAULT 0,
                    weather_complete              INTEGER NOT NULL DEFAULT 0,

                    computed_at                   TEXT NOT NULL
                        DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
                )
            """)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_daily_summary_date "
                "ON daily_summary(date)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_daily_summary_country "
                "ON daily_summary(country_code)"
            )
            try:
                conn.execute("ALTER TABLE daily_summary ADD COLUMN uv_index_max REAL")
            except Exception:
                pass  # Column already exists
    
    def init_complete_view(self):
        with get_conn() as conn:
            conn.execute("""
                CREATE VIEW IF NOT EXISTS daily_summary_complete AS
                SELECT * FROM daily_summary
                WHERE health_complete   = 1
                    AND location_complete = 1
                    AND pi_complete       = 1
                    AND spend_complete    = 1
                    AND weather_complete  = 1
            """)

    def insert(self, record: DailySummaryRecord) -> None:
        """Insert or replace a daily summary row.

        Uses INSERT OR REPLACE so re-running for a past date with updated
        source data (e.g. late Revolut upload) overwrites the stale row.
        Does not return a bool — upsert always succeeds or raises.
        """
        with get_conn() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO daily_summary (
                    date, timezone, utc_offset, utc_start, utc_end,
                    country_code, country, region, city,
                    dominant_place_id, dominant_known_place_id,
                    location_points, overland_points, overland_coverage_pct,
                    distinct_places, new_places_visited, was_in_transit,
                    steps, active_energy_kcal, resting_energy_kcal,
                    distance_km, flights_climbed, time_in_daylight_min,
                    avg_walking_speed_kmh, vo2_max,
                    resting_hr, avg_hrv_ms, avg_spo2_pct,
                    respiratory_rate, wrist_temp_c,
                    wake_time_local, sleep_time_local,
                    sleep_hours, awake_hours, deep_sleep_hours,
                    rem_sleep_hours, light_sleep_hours,
                    sleep_efficiency_pct, restorative_sleep_pct,
                    avg_valence, mood_entries,
                    spend_gbp, spend_local, spend_currency,
                    transaction_count, spend_normalised,
                    temp_max_c, temp_min_c, precipitation_mm, weathercode,
                    photo_count,
                    watchdog_heartbeats_received, watchdog_max_gap_mins,
                    watchdog_max_consecutive_fail,
                    travelnet_internet_ok_pct, travelnet_api_ok_pct, prefect_ok_pct,
                    avg_w_pi, total_wh_pi
                ) VALUES (
                    ?, ?, ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?,
                    ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?,
                    ?, ?,
                    ?, ?, ?,
                    ?, ?,
                    ?, ?,
                    ?, ?, ?,
                    ?, ?,
                    ?, ?, ?,
                    ?, ?,
                    ?, ?, ?,
                    ?, ?,
                    ?, ?, ?, ?,
                    ?,
                    ?, ?,
                    ?,
                    ?, ?,
                    ?, ?
                )
            """, (
                record.date, record.timezone, record.utc_offset,
                record.utc_start, record.utc_end,
                record.country_code, record.country, record.region, record.city,
                record.dominant_place_id, record.dominant_known_place_id,
                record.location_points, record.overland_points,
                record.overland_coverage_pct,
                record.distinct_places, record.new_places_visited,
                int(record.was_in_transit) if record.was_in_transit is not None else 0,
                record.steps, record.active_energy_kcal, record.resting_energy_kcal,
                record.distance_km, record.flights_climbed, record.time_in_daylight_min,
                record.avg_walking_speed_kmh, record.vo2_max,
                record.resting_hr, record.avg_hrv_ms, record.avg_spo2_pct,
                record.respiratory_rate, record.wrist_temp_c,
                record.wake_time_local, record.sleep_time_local,
                record.sleep_hours, record.awake_hours, record.deep_sleep_hours,
                record.rem_sleep_hours, record.light_sleep_hours,
                record.sleep_efficiency_pct, record.restorative_sleep_pct,
                record.avg_valence, record.mood_entries,
                record.spend_gbp, record.spend_local, record.spend_currency,
                record.transaction_count, record.spend_normalised,
                record.temp_max_c, record.temp_min_c,
                record.precipitation_mm, record.weathercode,
                record.photo_count,
                record.watchdog_heartbeats_received, record.watchdog_max_gap_mins,
                record.watchdog_max_consecutive_fail,
                record.travelnet_internet_ok_pct, record.travelnet_api_ok_pct,
                record.prefect_ok_pct, record.avg_w_pi, record.total_wh_pi,
            ))


table = DailySummaryTable()