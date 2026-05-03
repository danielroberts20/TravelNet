"""
database/ml/day_embeddings.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Per-day embedding vectors and UMAP projections from the autoencoder pipeline.
Populated post-departure by the ML training phase.
"""
from dataclasses import dataclass

from database.base import BaseTable
from database.connection import get_conn


@dataclass
class MlDayEmbeddingRecord:
    pass


class MlDayEmbeddingsTable(BaseTable[MlDayEmbeddingRecord]):

    def init(self) -> None:
        with get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ml_day_embeddings (
                    date                 TEXT PRIMARY KEY,
                    embedding_json       TEXT NOT NULL,
                    umap_x               REAL,
                    umap_y               REAL,
                    reconstruction_error REAL,
                    cluster_label        TEXT,
                    model_version        TEXT NOT NULL,
                    created_at           TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
                )
            """)

    def insert(self, record: MlDayEmbeddingRecord) -> None:
        pass


table = MlDayEmbeddingsTable()
