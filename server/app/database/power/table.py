from dataclasses import dataclass
from database.base import BaseTable
from database.connection import get_conn


@dataclass
class PowerDailyRecord:
    date: str
    min_w: float
    max_w: float
    avg_w: float
    readings: int
    start_wh: float   # aenergy.total at first reading of the day
    end_wh: float     # aenergy.total at latest reading of the day

    @property
    def total_wh(self) -> float:
        return round(self.end_wh - self.start_wh, 3)


_UPSERT_SQL = """
    INSERT INTO power_daily (date, min_w, max_w, avg_w, readings, start_wh, end_wh)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(date) DO UPDATE SET
        min_w    = excluded.min_w,
        max_w    = excluded.max_w,
        avg_w    = excluded.avg_w,
        readings = excluded.readings,
        end_wh   = excluded.end_wh
"""

_EXISTING_COLS = ("min_w", "max_w", "avg_w", "readings", "start_wh")


def merge_reading(existing: dict | None, date: str, watts: float, energy_total: float) -> PowerDailyRecord:
    """Fold one Shelly reading into the day's running aggregate (pure function)."""
    if existing is None:
        w = round(watts, 2)
        return PowerDailyRecord(
            date=date, min_w=w, max_w=w, avg_w=w, readings=1,
            start_wh=energy_total, end_wh=energy_total,
        )
    n = existing["readings"]
    return PowerDailyRecord(
        date=date,
        min_w=round(min(existing["min_w"], watts), 2),
        max_w=round(max(existing["max_w"], watts), 2),
        avg_w=round((existing["avg_w"] * n + watts) / (n + 1), 2),
        readings=n + 1,
        start_wh=existing["start_wh"],
        end_wh=energy_total,
    )


class PowerDailyTable(BaseTable[PowerDailyRecord]):
    def init(self) -> None:
        with get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS power_daily (
                    id         INTEGER PRIMARY KEY AUTOINCREMENT,
                    date       TEXT NOT NULL UNIQUE,
                    min_w      REAL NOT NULL,
                    max_w      REAL NOT NULL,
                    avg_w      REAL NOT NULL,
                    readings   INTEGER NOT NULL,
                    start_wh   REAL NOT NULL,
                    end_wh     REAL NOT NULL,
                    total_wh   REAL GENERATED ALWAYS AS (round(end_wh - start_wh, 3)) VIRTUAL,
                    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_power_daily_date
                ON power_daily(date)
            """)

    def insert(self, record: PowerDailyRecord) -> bool:
        """Upsert a daily power aggregate. Safe to call multiple times per day
        — updates min/max/avg/readings each time with latest data."""
        with get_conn() as conn:
            conn.execute(_UPSERT_SQL, (
                record.date,
                record.min_w,
                record.max_w,
                record.avg_w,
                record.readings,
                record.start_wh,
                record.end_wh,
            ))
        return True

    def upsert_reading(self, date: str, watts: float, energy_total: float, conn=None) -> PowerDailyRecord:
        """Atomically fold a reading into the day's row.

        The read-modify-write runs in a single BEGIN IMMEDIATE transaction, so a
        concurrent writer cannot slip in between the read and the write and have
        its update lost (and the write lock is taken up front, waiting on the
        connection's busy timeout instead of failing on a read->write upgrade).
        Pass ``conn`` to reuse an existing connection (tests); it is not closed.
        """
        own = conn is None
        if own:
            conn = get_conn()
        prev_isolation = conn.isolation_level
        conn.isolation_level = None  # explicit transaction control
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT min_w, max_w, avg_w, readings, start_wh FROM power_daily WHERE date = ?",
                (date,),
            ).fetchone()
            existing = dict(zip(_EXISTING_COLS, row)) if row else None
            record = merge_reading(existing, date, watts, energy_total)
            conn.execute(_UPSERT_SQL, (
                record.date, record.min_w, record.max_w, record.avg_w,
                record.readings, record.start_wh, record.end_wh,
            ))
            conn.execute("COMMIT")
            return record
        except Exception:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.isolation_level = prev_isolation
            if own:
                conn.close()


table = PowerDailyTable()