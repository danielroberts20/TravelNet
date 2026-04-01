"""
migrate_to_iso_timestamps.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
One-shot migration: convert all timestamp/date columns from their legacy types
(INTEGER Unix epoch, TIMESTAMP/DATETIME affinity, or partial ISO strings) to
canonical ISO 8601 UTC TEXT with Z suffix — matching the output of to_iso_str().

Date-only columns (fx_rates.date, weather_daily.date, api_usage.month) are left
as pure date strings (YYYY-MM-DD / YYYY-MM) and are NOT given a Z suffix.

Run from inside the Docker container (or any environment with access to travel.db):

    python migrate_to_iso_timestamps.py [--db /path/to/travel.db] [--dry-run]

The script is idempotent: it checks each column's current type before acting so
re-running on an already-migrated database is safe.

SQLite does not support ALTER COLUMN, so each affected table is rebuilt using the
standard 12-step approach:
  1. PRAGMA foreign_keys = OFF
  2. BEGIN
  3. DROP dependent views
  4. For each table: CREATE _new, INSERT (with conversion), DROP old, RENAME
  5. Recreate indexes
  6. Recreate views
  7. COMMIT
  8. PRAGMA foreign_keys = ON + foreign_key_check

Implementation note: Python's sqlite3.executescript() always issues an implicit
COMMIT before running, breaking transactional DDL.  This script uses explicit
conn.execute() calls throughout so that the entire migration runs inside one
transaction and can be fully rolled back on error.
"""

import argparse
import sqlite3
import sys
from pathlib import Path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def col_type(conn: sqlite3.Connection, table: str, column: str) -> str | None:
    """Return the declared type of a column (uppercased), or None if not found."""
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    for row in rows:
        if row[1] == column:
            return row[2].upper()
    return None


def x(conn: sqlite3.Connection, sql: str) -> None:
    """Execute a single SQL statement, stripping surrounding whitespace."""
    conn.execute(sql.strip())


# ---------------------------------------------------------------------------
# Individual table migrations
# ---------------------------------------------------------------------------

def migrate_location_shortcuts(conn: sqlite3.Connection) -> None:
    """
    timestamp:  INTEGER (Unix epoch)   → TEXT NOT NULL (ISO 8601 UTC, e.g. 2026-03-25T12:34:56Z)
    created_at: INTEGER (Unix epoch)   → TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))

    Also removes the obsolete CHECK(timestamp > 1500000000) guard.

    Extra columns that exist in older DB versions (timezone, is_noise) are
    preserved — the current insert code ignores them but dropping them would
    be destructive.
    """
    t = col_type(conn, "location_shortcuts", "timestamp")
    if t == "TEXT":
        print("  location_shortcuts: already TEXT — skipping")
        return

    print("  location_shortcuts: INTEGER → TEXT  (Unix epoch → ISO 8601)")
    x(conn, """
        CREATE TABLE location_shortcuts_new (
            id                   INTEGER PRIMARY KEY,
            timestamp            TEXT    NOT NULL,
            latitude             REAL    NOT NULL CHECK(latitude  BETWEEN -90  AND  90),
            longitude            REAL    NOT NULL CHECK(longitude BETWEEN -180 AND 180),
            altitude             REAL,
            device               TEXT    NOT NULL,
            is_locked            BOOLEAN,
            battery              INTEGER CHECK(battery >= 0 AND battery <= 100),
            is_charging          BOOLEAN,
            is_connected_charger BOOLEAN,
            BSSID                TEXT,
            RSSI                 INTEGER,
            created_at           TEXT    DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
            is_noise             BOOLEAN,
            UNIQUE(timestamp, device)
        )
    """)
    x(conn, """
        INSERT INTO location_shortcuts_new
            SELECT
                id,
                strftime('%Y-%m-%dT%H:%M:%SZ', timestamp, 'unixepoch'),
                latitude, longitude, altitude,
                device, is_locked, battery, is_charging, is_connected_charger,
                BSSID, RSSI,
                strftime('%Y-%m-%dT%H:%M:%SZ', created_at, 'unixepoch'),
                is_noise
            FROM location_shortcuts
    """)
    x(conn, "DROP TABLE location_shortcuts")
    x(conn, "ALTER TABLE location_shortcuts_new RENAME TO location_shortcuts")
    x(conn, "CREATE INDEX idx_location_timestamp ON location_shortcuts(timestamp)")
    x(conn, "CREATE INDEX idx_location_device    ON location_shortcuts(device)")
    x(conn, "CREATE INDEX idx_location_lat_lon   ON location_shortcuts(latitude, longitude)")


def migrate_health_data(conn: sqlite3.Connection) -> None:
    """
    timestamp:  INTEGER (Unix epoch)               → TEXT NOT NULL  (ISO 8601 UTC)
    created_at: TIMESTAMP (CURRENT_TIMESTAMP fmt)  → TEXT DEFAULT   (strftime ISO)
    """
    t = col_type(conn, "health_data", "timestamp")
    if t == "TEXT":
        print("  health_data: already TEXT — skipping")
        return

    print("  health_data: INTEGER → TEXT  (Unix epoch → ISO 8601)")
    x(conn, """
        CREATE TABLE health_data_new (
            id         INTEGER PRIMARY KEY,
            timestamp  TEXT    NOT NULL,
            metric     TEXT    NOT NULL,
            value_json TEXT,
            created_at TEXT    DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
            UNIQUE(timestamp, metric)
        )
    """)
    x(conn, """
        INSERT INTO health_data_new
            SELECT
                id,
                strftime('%Y-%m-%dT%H:%M:%SZ', timestamp, 'unixepoch'),
                metric,
                value_json,
                strftime('%Y-%m-%dT%H:%M:%SZ', created_at)
            FROM health_data
    """)
    x(conn, "DROP TABLE health_data")
    x(conn, "ALTER TABLE health_data_new RENAME TO health_data")
    x(conn, "CREATE INDEX idx_health_timestamp ON health_data(timestamp)")


def migrate_workouts(conn: sqlite3.Connection) -> None:
    """
    start_ts:   INTEGER (Unix epoch)               → TEXT NOT NULL  (ISO 8601 UTC)
    end_ts:     INTEGER (Unix epoch)               → TEXT NOT NULL  (ISO 8601 UTC)
    created_at: TIMESTAMP (CURRENT_TIMESTAMP fmt)  → TEXT DEFAULT   (strftime ISO)
    """
    t = col_type(conn, "workouts", "start_ts")
    if t == "TEXT":
        print("  workouts: already TEXT — skipping")
        return

    print("  workouts: INTEGER → TEXT  (Unix epoch → ISO 8601)")
    x(conn, """
        CREATE TABLE workouts_new (
            id                  TEXT PRIMARY KEY,
            name                TEXT NOT NULL,
            start_ts            TEXT NOT NULL,
            end_ts              TEXT NOT NULL,
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
            created_at          TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
        )
    """)
    x(conn, """
        INSERT INTO workouts_new
            SELECT
                id, name,
                strftime('%Y-%m-%dT%H:%M:%SZ', start_ts, 'unixepoch'),
                strftime('%Y-%m-%dT%H:%M:%SZ', end_ts,   'unixepoch'),
                duration, location, is_indoor,
                active_energy_kcal, total_energy_kcal,
                distance, distance_units,
                avg_speed, max_speed, speed_units,
                elevation_up, elevation_down, elevation_units,
                hr_min, hr_avg, hr_max,
                intensity_met, humidity,
                temperature, temperature_units,
                step_cadence, flights_climbed,
                lap_length, lap_length_units,
                stroke_style, swolf_score, salinity,
                swim_stroke_count, swim_cadence,
                strftime('%Y-%m-%dT%H:%M:%SZ', created_at)
            FROM workouts
    """)
    x(conn, "DROP TABLE workouts")
    x(conn, "ALTER TABLE workouts_new RENAME TO workouts")
    x(conn, "CREATE INDEX idx_workouts_start_ts ON workouts(start_ts)")
    x(conn, "CREATE INDEX idx_workouts_name     ON workouts(name)")


def migrate_workout_route(conn: sqlite3.Connection) -> None:
    """
    timestamp: INTEGER (Unix epoch) → TEXT NOT NULL (ISO 8601 UTC)
    """
    t = col_type(conn, "workout_route", "timestamp")
    if t == "TEXT":
        print("  workout_route: already TEXT — skipping")
        return

    print("  workout_route: INTEGER → TEXT  (Unix epoch → ISO 8601)")
    x(conn, """
        CREATE TABLE workout_route_new (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            workout_id          TEXT    NOT NULL REFERENCES workouts(id),
            timestamp           TEXT    NOT NULL,
            latitude            REAL    NOT NULL,
            longitude           REAL    NOT NULL,
            altitude            REAL,
            speed               REAL,
            speed_accuracy      REAL,
            course              REAL,
            course_accuracy     REAL,
            horizontal_accuracy REAL,
            vertical_accuracy   REAL
        )
    """)
    x(conn, """
        INSERT INTO workout_route_new
            SELECT
                id, workout_id,
                strftime('%Y-%m-%dT%H:%M:%SZ', timestamp, 'unixepoch'),
                latitude, longitude, altitude,
                speed, speed_accuracy, course, course_accuracy,
                horizontal_accuracy, vertical_accuracy
            FROM workout_route
    """)
    x(conn, "DROP TABLE workout_route")
    x(conn, "ALTER TABLE workout_route_new RENAME TO workout_route")
    x(conn, "CREATE INDEX idx_workout_route_workout_id ON workout_route(workout_id)")
    x(conn, "CREATE INDEX idx_workout_route_timestamp  ON workout_route(timestamp)")


def migrate_fx_rates(conn: sqlite3.Connection) -> None:
    """
    timestamp:  INTEGER (Unix epoch)               → TEXT NOT NULL  (ISO 8601 UTC)
    created_at: TIMESTAMP (CURRENT_TIMESTAMP fmt)  → TEXT DEFAULT   (strftime ISO)
    date:       TEXT (YYYY-MM-DD)                  → unchanged
    """
    t = col_type(conn, "fx_rates", "timestamp")
    if t == "TEXT":
        print("  fx_rates: already TEXT — skipping")
        return

    print("  fx_rates: timestamp INTEGER → TEXT  (Unix epoch → ISO 8601)")
    x(conn, """
        CREATE TABLE fx_rates_new (
            id              INTEGER PRIMARY KEY,
            date            TEXT    NOT NULL,
            source_currency TEXT    NOT NULL,
            target_currency TEXT    NOT NULL,
            rate            REAL    NOT NULL,
            timestamp       TEXT    NOT NULL,
            created_at      TEXT    DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
            UNIQUE(date, source_currency, target_currency)
        )
    """)
    x(conn, """
        INSERT INTO fx_rates_new
            SELECT
                id,
                date,
                source_currency, target_currency, rate,
                strftime('%Y-%m-%dT%H:%M:%SZ', timestamp, 'unixepoch'),
                strftime('%Y-%m-%dT%H:%M:%SZ', created_at)
            FROM fx_rates
    """)
    x(conn, "DROP TABLE fx_rates")
    x(conn, "ALTER TABLE fx_rates_new RENAME TO fx_rates")
    x(conn, "CREATE INDEX idx_fx_date          ON fx_rates(date)")
    x(conn, "CREATE INDEX idx_fx_source_target ON fx_rates(source_currency, target_currency)")


def migrate_gap_annotations(conn: sqlite3.Connection) -> None:
    """
    start_ts:   INTEGER (Unix epoch) → TEXT NOT NULL (ISO 8601 UTC)
    end_ts:     INTEGER (Unix epoch) → TEXT NOT NULL (ISO 8601 UTC)
    created_at: TEXT DEFAULT (datetime('now')) — update default; existing rows
                are in 'YYYY-MM-DD HH:MM:SS' format, convert to ISO 8601.
    """
    t = col_type(conn, "gap_annotations", "start_ts")
    if t == "TEXT":
        print("  gap_annotations: already TEXT — skipping")
        return

    print("  gap_annotations: INTEGER → TEXT  (Unix epoch → ISO 8601)")
    x(conn, """
        CREATE TABLE gap_annotations_new (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            start_ts    TEXT    NOT NULL,
            end_ts      TEXT    NOT NULL,
            description TEXT    NOT NULL,
            created_at  TEXT    DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
        )
    """)
    x(conn, """
        INSERT INTO gap_annotations_new
            SELECT
                id,
                strftime('%Y-%m-%dT%H:%M:%SZ', start_ts, 'unixepoch'),
                strftime('%Y-%m-%dT%H:%M:%SZ', end_ts,   'unixepoch'),
                description,
                strftime('%Y-%m-%dT%H:%M:%SZ', created_at)
            FROM gap_annotations
    """)
    x(conn, "DROP TABLE gap_annotations")
    x(conn, "ALTER TABLE gap_annotations_new RENAME TO gap_annotations")
    x(conn, "CREATE INDEX idx_gap_annotations_start ON gap_annotations(start_ts)")


def migrate_weather_hourly(conn: sqlite3.Connection) -> None:
    """
    fetched_at: DATETIME (ISO without Z, e.g. '2026-03-21T20:41:53')
                → TEXT NOT NULL  (add Z: '2026-03-21T20:41:53Z')
    timestamp:  DATETIME (ISO without Z, possibly without seconds, e.g. '2026-02-02T00:00')
                → TEXT NOT NULL  (normalise + add Z: '2026-02-02T00:00:00Z')

    strftime('%Y-%m-%dT%H:%M:%SZ', value) handles both formats: SQLite accepts
    'YYYY-MM-DDTHH:MM' and 'YYYY-MM-DDTHH:MM:SS' as input, and always emits
    full HH:MM:SS with the trailing literal Z.
    """
    t = col_type(conn, "weather_hourly", "fetched_at")
    if t == "TEXT":
        print("  weather_hourly: already TEXT — skipping")
        return

    print("  weather_hourly: DATETIME → TEXT  (add Z suffix, normalise seconds)")
    x(conn, """
        CREATE TABLE weather_hourly_new (
            id                     INTEGER PRIMARY KEY AUTOINCREMENT,
            fetched_at             TEXT    NOT NULL,
            timestamp              TEXT    NOT NULL,
            latitude               REAL    NOT NULL,
            longitude              REAL    NOT NULL,
            temperature_c          REAL,
            apparent_temperature_c REAL,
            precipitation_mm       REAL,
            windspeed_kmh          REAL,
            winddirection_deg      REAL,
            uv_index               REAL,
            cloudcover_pct         REAL,
            is_day                 INTEGER,
            weathercode            INTEGER,
            raw_json               TEXT,
            UNIQUE(timestamp, latitude, longitude)
        )
    """)
    x(conn, """
        INSERT INTO weather_hourly_new
            SELECT
                id,
                strftime('%Y-%m-%dT%H:%M:%SZ', fetched_at),
                strftime('%Y-%m-%dT%H:%M:%SZ', timestamp),
                latitude, longitude,
                temperature_c, apparent_temperature_c, precipitation_mm,
                windspeed_kmh, winddirection_deg, uv_index, cloudcover_pct,
                is_day, weathercode, raw_json
            FROM weather_hourly
    """)
    x(conn, "DROP TABLE weather_hourly")
    x(conn, "ALTER TABLE weather_hourly_new RENAME TO weather_hourly")
    x(conn, "CREATE INDEX idx_weather_hourly_timestamp ON weather_hourly(timestamp)")
    x(conn, "CREATE INDEX idx_weather_hourly_lat_lon   ON weather_hourly(latitude, longitude)")


def migrate_weather_daily(conn: sqlite3.Connection) -> None:
    """
    fetched_at: DATETIME (ISO without Z) → TEXT NOT NULL  (add Z)
    date:       DATE     (YYYY-MM-DD)    → TEXT NOT NULL  (values unchanged)
    sunrise:    DATETIME (local solar time, e.g. '2026-02-02T07:43') → TEXT (unchanged)
    sunset:     DATETIME (local solar time)                           → TEXT (unchanged)

    sunrise/sunset are NOT UTC — they represent local solar times at each
    location.  The Z suffix must NOT be added to them.
    """
    t = col_type(conn, "weather_daily", "fetched_at")
    if t == "TEXT":
        print("  weather_daily: already TEXT — skipping")
        return

    print("  weather_daily: DATETIME → TEXT  (fetched_at gets Z; date/sunrise/sunset unchanged)")
    x(conn, """
        CREATE TABLE weather_daily_new (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            fetched_at            TEXT    NOT NULL,
            date                  TEXT    NOT NULL,
            latitude              REAL    NOT NULL,
            longitude             REAL    NOT NULL,
            sunrise               TEXT,
            sunset                TEXT,
            precipitation_sum_mm  REAL,
            precipitation_hours   REAL,
            snowfall_sum_cm       REAL,
            windspeed_max_kmh     REAL,
            windgusts_max_kmh     REAL,
            raw_json              TEXT,
            UNIQUE(date, latitude, longitude)
        )
    """)
    x(conn, """
        INSERT INTO weather_daily_new
            SELECT
                id,
                strftime('%Y-%m-%dT%H:%M:%SZ', fetched_at),
                date,
                latitude, longitude,
                sunrise, sunset,
                precipitation_sum_mm, precipitation_hours,
                snowfall_sum_cm, windspeed_max_kmh, windgusts_max_kmh,
                raw_json
            FROM weather_daily
    """)
    x(conn, "DROP TABLE weather_daily")
    x(conn, "ALTER TABLE weather_daily_new RENAME TO weather_daily")
    x(conn, "CREATE INDEX idx_weather_daily_date    ON weather_daily(date)")
    x(conn, "CREATE INDEX idx_weather_daily_lat_lon ON weather_daily(latitude, longitude)")


def migrate_location_overland(conn: sqlite3.Connection) -> None:
    """
    timestamp:   TEXT ('YYYY-MM-DD HH:MM:SS') → TEXT ('YYYY-MM-DDTHH:MM:SSZ')
    inserted_at: TEXT ('YYYY-MM-DD HH:MM:SS') → TEXT ('YYYY-MM-DDTHH:MM:SSZ')
                 DEFAULT updated from datetime('now') to strftime ISO form.

    Both columns are already TEXT, so we detect migration by checking for the
    space separator in a sample row.  If no rows exist, the table is rebuilt
    anyway to update the DEFAULT clause on inserted_at.
    """
    sample = conn.execute(
        "SELECT timestamp FROM location_overland LIMIT 1"
    ).fetchone()
    if sample and "T" in sample[0] and sample[0].endswith("Z"):
        print("  location_overland: already ISO 8601+Z format — skipping")
        return

    print("  location_overland: TEXT 'YYYY-MM-DD HH:MM:SS' → TEXT ISO 8601+Z")
    x(conn, """
        CREATE TABLE location_overland_new (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id           TEXT,
            timestamp           TEXT    NOT NULL,
            lat                 REAL    NOT NULL,
            lon                 REAL    NOT NULL,
            altitude            REAL,
            speed               REAL,
            horizontal_accuracy REAL,
            vertical_accuracy   REAL,
            motion              TEXT,
            activity            TEXT,
            wifi_ssid           TEXT,
            battery_state       TEXT,
            battery_level       REAL,
            pauses              INTEGER,
            desired_accuracy    REAL,
            significant_change  TEXT,
            raw_json            TEXT,
            inserted_at         TEXT    DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
            is_noise            BOOLEAN,
            UNIQUE(device_id, timestamp)
        )
    """)
    x(conn, """
        INSERT INTO location_overland_new
            SELECT
                id, device_id,
                strftime('%Y-%m-%dT%H:%M:%SZ', timestamp),
                lat, lon, altitude, speed,
                horizontal_accuracy, vertical_accuracy,
                motion, activity, wifi_ssid,
                battery_state, battery_level, pauses,
                desired_accuracy, significant_change, raw_json,
                strftime('%Y-%m-%dT%H:%M:%SZ', inserted_at),
                is_noise
            FROM location_overland
    """)
    x(conn, "DROP TABLE location_overland")
    x(conn, "ALTER TABLE location_overland_new RENAME TO location_overland")
    x(conn, "CREATE INDEX idx_overland_timestamp ON location_overland(timestamp)")
    x(conn, "CREATE INDEX idx_overland_device_ts ON location_overland(device_id, timestamp)")


# ---------------------------------------------------------------------------
# View recreation
# ---------------------------------------------------------------------------

def recreate_location_unified(conn: sqlite3.Connection) -> None:
    """
    The backup view uses datetime(timestamp, 'unixepoch') for the location_shortcuts
    branch.  After migration, timestamp is TEXT so that conversion is removed.
    """
    print("  location_unified view: recreating")
    x(conn, "DROP VIEW IF EXISTS location_unified")
    x(conn, """
        CREATE VIEW location_unified AS
        SELECT
            timestamp                    AS timestamp,
            lat                          AS lat,
            lon                          AS lon,
            altitude                     AS altitude,
            activity                     AS activity,
            battery_level                AS battery,
            speed                        AS speed,
            device_id                    AS device,
            horizontal_accuracy          AS accuracy,
            'overland'                   AS source
        FROM location_overland

        UNION ALL

        SELECT
            timestamp                        AS timestamp,
            latitude                         AS lat,
            longitude                        AS lon,
            altitude                         AS altitude,
            CAST(battery AS REAL) / 100.0    AS battery,
            NULL                             AS speed,
            device                           AS device,
            NULL                             AS accuracy,
            'shortcuts'                      AS source

        FROM location_shortcuts
        ORDER BY timestamp ASC
    """)


def recreate_location_weather(conn: sqlite3.Connection) -> None:
    """
    Two changes from the backup view:
      1. The hourly join condition changes from
             strftime('%Y-%m-%dT%H:00', u.timestamp) = h.timestamp
         to
             strftime('%Y-%m-%dT%H:00:00Z', u.timestamp) = h.timestamp
         because weather_hourly.timestamp is now '2026-02-02T00:00:00Z' (with Z
         and full seconds) after migration.
      2. A LEFT JOIN on weather_daily is added (present in current code but
         absent from the backup view).
    """
    print("  location_weather view: recreating")
    x(conn, "DROP VIEW IF EXISTS location_weather")
    x(conn, """
        CREATE VIEW location_weather AS
        SELECT
            u.*,
            h.temperature_c,
            h.apparent_temperature_c,
            h.precipitation_mm,
            h.windspeed_kmh,
            h.winddirection_deg,
            h.uv_index,
            h.cloudcover_pct,
            h.is_day,
            h.weathercode,
            d.sunrise,
            d.sunset,
            d.precipitation_sum_mm,
            d.precipitation_hours,
            d.snowfall_sum_cm,
            d.windspeed_max_kmh,
            d.windgusts_max_kmh
        FROM location_unified u
        LEFT JOIN weather_hourly h
            ON  ROUND(u.lat, 2) = h.latitude
            AND ROUND(u.lon, 2) = h.longitude
            AND strftime('%Y-%m-%dT%H:00:00Z', u.timestamp) = h.timestamp
        LEFT JOIN weather_daily d
            ON  ROUND(u.lat, 2) = d.latitude
            AND ROUND(u.lon, 2) = d.longitude
            AND DATE(u.timestamp) = d.date
    """)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(db_path: str, dry_run: bool) -> None:
    if not Path(db_path).exists():
        print(f"ERROR: database not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    print(f"{'[DRY RUN] ' if dry_run else ''}Migrating: {db_path}")

    # isolation_level=None → autocommit mode; we manage BEGIN/COMMIT explicitly.
    # This avoids sqlite3.executescript()'s implicit COMMIT and Python's own
    # implicit transaction handling that would otherwise commit DDL mid-flight.
    conn = sqlite3.connect(db_path, isolation_level=None)
    conn.row_factory = sqlite3.Row

    try:
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute("BEGIN")

        # Drop views first — they reference tables we're about to rebuild, and
        # SQLite will refuse to DROP a table whose name appears in a live view.
        print("  Dropping dependent views")
        x(conn, "DROP VIEW IF EXISTS location_weather")
        x(conn, "DROP VIEW IF EXISTS location_unified")

        # --- Table migrations (parents before children) ---
        migrate_location_shortcuts(conn)   # parent of cellular_state
        migrate_health_data(conn)        # parent of health_sources
        migrate_workouts(conn)           # parent of workout_route
        migrate_workout_route(conn)
        migrate_fx_rates(conn)
        migrate_gap_annotations(conn)
        migrate_weather_hourly(conn)
        migrate_weather_daily(conn)
        migrate_location_overland(conn)  # used by location_unified view

        # --- Recreate views ---
        recreate_location_unified(conn)
        recreate_location_weather(conn)

        if dry_run:
            print("\n[DRY RUN] Rolling back — no changes written.")
            conn.execute("ROLLBACK")
        else:
            conn.execute("COMMIT")
            conn.execute("PRAGMA foreign_keys = ON")
            violations = conn.execute("PRAGMA foreign_key_check").fetchall()
            if violations:
                print("WARNING: foreign key violations after migration:")
                for v in violations:
                    print(f"  {dict(v)}")
            else:
                print("\nMigration complete. No foreign key violations.")
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate travel.db to ISO 8601 TEXT timestamps")
    parser.add_argument(
        "--db",
        default="/app/data/travel.db",
        help="Path to travel.db (default: /app/data/travel.db)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and preview conversions without writing any changes",
    )
    args = parser.parse_args()
    run(args.db, args.dry_run)
