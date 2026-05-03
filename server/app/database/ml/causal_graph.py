"""
database/ml/causal_graph.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Causal graph edges from the PC-algorithm / bootstrap causal discovery pipeline.
Populated post-departure by the ML training phase.
"""
from dataclasses import dataclass

from database.base import BaseTable
from database.connection import get_conn


@dataclass
class MlCausalGraphRecord:
    pass


class MlCausalGraphTable(BaseTable[MlCausalGraphRecord]):

    def init(self) -> None:
        with get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ml_causal_graph (
                    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
                    version              TEXT NOT NULL,
                    edge_from            TEXT NOT NULL,
                    edge_to              TEXT NOT NULL,
                    bootstrap_confidence REAL,
                    effect_sign          INTEGER,
                    created_at           TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
                    UNIQUE(version, edge_from, edge_to)
                )
            """)

    def insert(self, record: MlCausalGraphRecord) -> None:
        pass


table = MlCausalGraphTable()
