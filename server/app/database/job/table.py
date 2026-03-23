"""
database/job/table.py
~~~~~~~~~~~~~~~~~~~~~
Schema and CRUD helpers for the jobs table.

Jobs represent ML/analysis tasks submitted by a worker client.  A job moves
through the states: QUEUED → RUNNING → COMPLETED | FAILED.
"""

from datetime import datetime

from jobs.models import DataMode, Job, Status
from database.util import get_conn


def init() -> None:
    """Create the jobs table and its indexes if they do not exist."""
    with get_conn() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY,
        status TEXT NOT NULL,
        created_at TEXT NOT NULL,
        started_at TEXT,
        finished_at TEXT,

        code_path TEXT NOT NULL,
        requirements_path TEXT NOT NULL,

        data_mode TEXT NOT NULL,
        data_path TEXT,
        sql_query TEXT,

        entry_point TEXT NOT NULL,
        timeout INTEGER NOT NULL,

        worker_id TEXT,
        error_message TEXT
        );
        """)

        # Index for querying active jobs by status + creation order
        conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_jobs_status_created
        ON jobs (status, created_at);""")


def insert_job(job: Job) -> None:
    """Persist a new Job to the database."""
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO jobs (
                id, status, created_at,
                code_path, requirements_path,
                data_mode, data_path, sql_query,
                entry_point, timeout
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            job.id,
            job.status.value,
            job.created_at.isoformat(),
            job.code_path,
            job.requirements_path,
            job.data_mode.value,
            job.data_path,
            job.sql_query,
            job.entry_point,
            job.timeout
        ))
        conn.commit()


def row_to_job(row) -> Job:
    """Convert a sqlite3.Row to a Job object."""
    return Job(
        id=row["id"],
        code_path=row["code_path"],
        requirements_path=row["requirements_path"],
        data_mode=DataMode(row["data_mode"]),
        data_path=row["data_path"],
        sql_query=row["sql_query"],
        entry_point=row["entry_point"],
        timeout=row["timeout"],
        status=Status(row["status"]),
        created_at=datetime.fromisoformat(row["created_at"]),
        started_at=datetime.fromisoformat(row["started_at"]) if row["started_at"] else None,
        finished_at=datetime.fromisoformat(row["finished_at"]) if row["finished_at"] else None,
        worker_id=row["worker_id"]
    )


def get_next_queued_job() -> Job | None:
    """Return the oldest QUEUED job from the DB, or None if the queue is empty."""
    with get_conn() as conn:
        row = conn.execute("""
            SELECT *
            FROM jobs
            WHERE status = ?
            ORDER BY created_at ASC
            LIMIT 1
        """, (Status.QUEUED.value,)).fetchone()

        if not row:
            return None

        return row_to_job(row)


def update_job(job_id: str, job: Job) -> None:
    """Persist all mutable fields of job back to the DB.

    Does nothing if job_id does not match job.id (safety guard against
    accidentally updating the wrong row).
    """
    if job_id != job.id:
        return
    with get_conn() as conn:
        conn.execute("""
            UPDATE jobs SET
                id = ?, status = ?, created_at = ?,
                code_path = ?, requirements_path = ?,
                data_mode = ?, data_path = ?, sql_query = ?,
                entry_point = ?, timeout = ?, started_at = ?, finished_at = ?, worker_id = ?
            WHERE id = ?
        """, (
            job.id,
            job.status.value,
            job.created_at.isoformat(),
            job.code_path,
            job.requirements_path,
            job.data_mode.value,
            job.data_path,
            job.sql_query,
            job.entry_point,
            job.timeout,
            job.started_at.isoformat() if job.started_at else None,
            job.finished_at.isoformat() if job.finished_at else None,
            job.worker_id,
            job_id
        ))
        conn.commit()
