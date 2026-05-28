"""
database/weather/fetch_log.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
WeatherFetchLog — per-cell, per-date fetch outcome log for the weather pipeline.

Tracks which (lat, lon, date) combinations have been successfully fetched from
Open-Meteo so the get_weather flow can skip already-complete cells and provide
the exact missing dates for partially-covered cells.
"""
from datetime import date, datetime, timedelta, timezone

from database.connection import get_conn, to_iso_str
from database.fetch_log_base import FetchLog


class WeatherFetchLog(FetchLog):
    """Manages the weather_fetch_log table (one row per cell per calendar date)."""

    def init(self) -> None:
        with get_conn() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS weather_fetch_log (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    fetched_at    TEXT NOT NULL,
                    latitude      REAL NOT NULL,
                    longitude     REAL NOT NULL,
                    date          TEXT NOT NULL,
                    hourly_ok     INTEGER NOT NULL DEFAULT 0,
                    uv_ok         INTEGER NOT NULL DEFAULT 0,
                    daily_ok      INTEGER NOT NULL DEFAULT 0,
                    hourly_rows   INTEGER,
                    daily_rows    INTEGER,
                    error_hourly  TEXT,
                    error_uv      TEXT,
                    error_daily   TEXT,
                    UNIQUE(latitude, longitude, date)
                );

                CREATE INDEX IF NOT EXISTS idx_weather_fetch_log_date
                    ON weather_fetch_log(date);

                CREATE INDEX IF NOT EXISTS idx_weather_fetch_log_lat_lon
                    ON weather_fetch_log(latitude, longitude);
            """)

    def record(
        self,
        lat: float,
        lon: float,
        date: date,
        hourly_ok: bool,
        uv_ok: bool,
        daily_ok: bool,
        hourly_rows: int | None = None,
        daily_rows: int | None = None,
        error_hourly: str | None = None,
        error_uv: str | None = None,
        error_daily: str | None = None,
    ) -> None:
        fetched_at = to_iso_str(datetime.now(timezone.utc))
        with get_conn() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO weather_fetch_log
                    (fetched_at, latitude, longitude, date,
                     hourly_ok, uv_ok, daily_ok,
                     hourly_rows, daily_rows,
                     error_hourly, error_uv, error_daily)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    fetched_at, lat, lon, date.isoformat(),
                    int(hourly_ok), int(uv_ok), int(daily_ok),
                    hourly_rows, daily_rows,
                    error_hourly, error_uv, error_daily,
                ),
            )

    def get_complete_cells(self, start_date: date, end_date: date) -> set[tuple[float, float]]:
        """Return (lat, lon) pairs where every date in the window is fully complete."""
        expected_days = (end_date - start_date).days + 1
        with get_conn(read_only=True) as conn:
            rows = conn.execute(
                """
                SELECT latitude, longitude
                FROM weather_fetch_log
                WHERE date BETWEEN ? AND ?
                  AND hourly_ok = 1
                  AND uv_ok     = 1
                  AND daily_ok  = 1
                GROUP BY latitude, longitude
                HAVING COUNT(DISTINCT date) = ?
                """,
                (start_date.isoformat(), end_date.isoformat(), expected_days),
            ).fetchall()
        return {(row["latitude"], row["longitude"]) for row in rows}

    def get_missing_dates_per_cell(
        self, start_date: date, end_date: date
    ) -> dict[tuple[float, float], list[date]]:
        """Return cells that have at least one covered date but are not fully complete.

        Maps (lat, lon) → sorted list of dates not yet logged as complete within
        the window. Cells with zero log entries are absent from the result — the
        caller falls back to all_dates for those.
        """
        all_dates = [
            start_date + timedelta(days=i)
            for i in range((end_date - start_date).days + 1)
        ]

        with get_conn(read_only=True) as conn:
            rows = conn.execute(
                """
                SELECT latitude, longitude, date
                FROM weather_fetch_log
                WHERE date BETWEEN ? AND ?
                  AND hourly_ok = 1 AND uv_ok = 1 AND daily_ok = 1
                """,
                (start_date.isoformat(), end_date.isoformat()),
            ).fetchall()

        covered: dict[tuple[float, float], set[str]] = {}
        for row in rows:
            key = (row["latitude"], row["longitude"])
            covered.setdefault(key, set()).add(row["date"])

        result: dict[tuple[float, float], list[date]] = {}
        for cell, covered_dates in covered.items():
            missing = [d for d in all_dates if d.isoformat() not in covered_dates]
            if missing:
                result[cell] = missing
        return result

    def prune(self, cutoff: date) -> int:
        with get_conn() as conn:
            return conn.execute(
                "DELETE FROM weather_fetch_log WHERE date < ?",
                (cutoff.isoformat(),),
            ).rowcount
    
    def seed_from_existing(self) -> dict:
        """Populate fetch log from weather data already in the DB.

        Safe to run after the log table is wiped (e.g. schema migration).
        Does not make any API calls. Returns a summary dict.
        """
        with get_conn() as conn:
            # Seed from hourly — daily_ok starts as 0
            conn.execute("""
                INSERT OR IGNORE INTO weather_fetch_log
                    (fetched_at, latitude, longitude, date,
                    hourly_ok, uv_ok, daily_ok, hourly_rows)
                SELECT
                    datetime('now'), latitude, longitude,
                    DATE(timestamp),
                    1,
                    CASE WHEN MAX(uv_index) IS NOT NULL THEN 1 ELSE 0 END,
                    0,
                    COUNT(*)
                FROM weather_hourly
                GROUP BY latitude, longitude, DATE(timestamp)
            """)

            # Update rows where daily data also exists
            conn.execute("""
                UPDATE weather_fetch_log
                SET daily_ok = 1,
                    daily_rows = (
                        SELECT COUNT(*) FROM weather_daily wd
                        WHERE wd.latitude  = weather_fetch_log.latitude
                        AND wd.longitude = weather_fetch_log.longitude
                        AND wd.date      = weather_fetch_log.date
                    )
                WHERE EXISTS (
                    SELECT 1 FROM weather_daily wd
                    WHERE wd.latitude  = weather_fetch_log.latitude
                    AND wd.longitude = weather_fetch_log.longitude
                    AND wd.date      = weather_fetch_log.date
                )
            """)

            row = conn.execute("""
                SELECT
                    COUNT(*)                                                      AS total,
                    SUM(hourly_ok)                                                AS hourly_ok,
                    SUM(daily_ok)                                                 AS daily_ok,
                    SUM(CASE WHEN hourly_ok = 1 AND uv_ok = 1
                                AND daily_ok = 1 THEN 1 ELSE 0 END)            AS fully_complete
                FROM weather_fetch_log
            """).fetchone()

        return {
            "total":          row["total"],
            "hourly_ok":      row["hourly_ok"],
            "daily_ok":       row["daily_ok"],
            "fully_complete": row["fully_complete"],
        }


weather_fetch_log = WeatherFetchLog()
