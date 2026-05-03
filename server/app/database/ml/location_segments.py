"""
database/ml/location_segments.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
ML-derived movement segments (transport mode classification).
Populated by the location segmentation pipeline post-departure.
"""
from dataclasses import dataclass

from database.base import BaseTable
from database.connection import get_conn


@dataclass
class MlLocationSegmentRecord:
    pass


class MlLocationSegmentsTable(BaseTable[MlLocationSegmentRecord]):

    def init(self) -> None:
        with get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ml_location_segments (
                    id             INTEGER PRIMARY KEY AUTOINCREMENT,
                    start_ts       TEXT NOT NULL,
                    end_ts         TEXT NOT NULL,
                    mode           TEXT NOT NULL,
                    distance_m     REAL,
                    duration_mins  REAL,
                    mean_speed_kph REAL,
                    confidence     REAL,
                    source         TEXT NOT NULL DEFAULT 'model',
                    model_version  TEXT,
                    created_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_ml_loc_seg_start_ts
                    ON ml_location_segments(start_ts)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_ml_loc_seg_end_ts
                    ON ml_location_segments(end_ts)
            """)

    def insert(self, record: MlLocationSegmentRecord) -> None:
        pass


table = MlLocationSegmentsTable()
