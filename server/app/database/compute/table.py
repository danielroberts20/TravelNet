"""
database/compute/table.py
~~~~~~~~~~~~~~~~~~~~~~~~~
Schema and CRUD helpers for the compute table.

Compute tasks represent ML/analysis tasks submitted by a worker client.
A task moves through the states: QUEUED → RUNNING → COMPLETED | FAILED.

The Compute dataclass (from compute/models.py) is used as the record type T,
since it already fully describes a compute task and is used throughout the
compute domain.
"""

from datetime import datetime

from compute.models import Compute, DataMode, Status
from database.base import BaseTable
from database.connection import get_conn, to_iso_str


class ComputeTable(BaseTable[Compute]):

    def init(self) -> None:
        """Create the compute table and its indexes if they do not exist."""
        with get_conn() as conn:
            conn.execute("""
            CREATE TABLE IF NOT EXISTS compute (
                id                  TEXT PRIMARY KEY,
                status              TEXT NOT NULL,
                created_at          TEXT NOT NULL,
                started_at          TEXT,
                finished_at         TEXT,
                code_path           TEXT NOT NULL,
                requirements_path   TEXT NOT NULL,
                data_mode           TEXT NOT NULL,
                data_path           TEXT,
                sql_query           TEXT,
                entry_point         TEXT NOT NULL,
                timeout_s           INTEGER NOT NULL,
                worker_id           TEXT,
                error_message       TEXT
            );
            """)

            conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_compute_status_created
            ON compute (status, created_at);""")

    def insert(self, record: Compute) -> None:
        """Persist a new Compute task to the database."""
        with get_conn() as conn:
            conn.execute("""
                INSERT INTO compute (
                    id, status, created_at,
                    code_path, requirements_path,
                    data_mode, data_path, sql_query,
                    entry_point, timeout_s
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                record.id,
                record.status.value,
                to_iso_str(record.created_at),
                record.code_path,
                record.requirements_path,
                record.data_mode.value,
                record.data_path,
                record.sql_query,
                record.entry_point,
                record.timeout,
            ))
            conn.commit()

    def update(self, compute_id: str, item: Compute) -> None:
        """Persist all mutable fields of a Compute task back to the DB.

        Does nothing if compute_id does not match item.id (safety guard against
        accidentally updating the wrong row).
        """
        if compute_id != item.id:
            return
        new_start = to_iso_str(item.started_at) if item.started_at else None
        new_finish = to_iso_str(item.finished_at) if item.finished_at else None
        with get_conn() as conn:
            conn.execute("""
                UPDATE compute SET
                    status = ?, created_at = ?,
                    code_path = ?, requirements_path = ?,
                    data_mode = ?, data_path = ?, sql_query = ?,
                    entry_point = ?, timeout_s = ?,
                    started_at = ?, finished_at = ?, worker_id = ?
                WHERE id = ?
            """, (
                item.status.value,
                to_iso_str(item.created_at),
                item.code_path,
                item.requirements_path,
                item.data_mode.value,
                item.data_path,
                item.sql_query,
                item.entry_point,
                item.timeout,
                new_start,
                new_finish,
                item.worker_id,
                compute_id,
            ))
            conn.commit()

    def get_next_queued(self) -> Compute | None:
        """Return the oldest QUEUED compute task, or None if the queue is empty."""
        with get_conn() as conn:
            row = conn.execute("""
                SELECT * FROM compute
                WHERE status = ?
                ORDER BY created_at ASC
                LIMIT 1
            """, (Status.QUEUED.value,)).fetchone()
            return self._row_to_compute(row) if row else None

    @staticmethod
    def _row_to_compute(row) -> Compute:
        """Convert a sqlite3.Row to a Compute object."""
        return Compute(
            id=row["id"],
            code_path=row["code_path"],
            requirements_path=row["requirements_path"],
            data_mode=DataMode(row["data_mode"]),
            data_path=row["data_path"],
            sql_query=row["sql_query"],
            entry_point=row["entry_point"],
            timeout=row["timeout_s"],
            status=Status(row["status"]),
            created_at=datetime.fromisoformat(row["created_at"]),
            started_at=datetime.fromisoformat(row["started_at"]) if row["started_at"] else None,
            finished_at=datetime.fromisoformat(row["finished_at"]) if row["finished_at"] else None,
            worker_id=row["worker_id"],
        )


table = ComputeTable()
