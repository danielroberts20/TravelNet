"""
database/ml/destination_profiles.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Per-destination feature profiles and cluster assignments.
Populated post-departure by the ML training phase.
"""
from dataclasses import dataclass

from database.base import BaseTable
from database.connection import get_conn


@dataclass
class MlDestinationProfileRecord:
    pass


class MlDestinationProfilesTable(BaseTable[MlDestinationProfileRecord]):

    def init(self) -> None:
        with get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ml_destination_profiles (
                    id             INTEGER PRIMARY KEY AUTOINCREMENT,
                    country_code   TEXT NOT NULL,
                    leg_start_date TEXT NOT NULL,
                    leg_end_date   TEXT,
                    features_json  TEXT NOT NULL,
                    cluster_label  TEXT,
                    model_version  TEXT,
                    created_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
                    UNIQUE(country_code, leg_start_date)
                )
            """)

    def insert(self, record: MlDestinationProfileRecord) -> None:
        pass


table = MlDestinationProfilesTable()
