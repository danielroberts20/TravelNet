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
            conn.execute("""
                INSERT INTO power_daily (date, min_w, max_w, avg_w, readings, start_wh, end_wh)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(date) DO UPDATE SET
                    min_w    = excluded.min_w,
                    max_w    = excluded.max_w,
                    avg_w    = excluded.avg_w,
                    readings = excluded.readings,
                    end_wh   = excluded.end_wh
            """, (
                record.date,
                record.min_w,
                record.max_w,
                record.avg_w,
                record.readings,
                record.start_wh,  # missing
                record.end_wh,    # missing
            ))
        return True


table = PowerDailyTable()