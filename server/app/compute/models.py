"""
compute/models.py
~~~~~~~~~~~~~~~~~
Domain models for the ML compute queue.

Compute tasks are submitted by a worker client, executed on a GPU worker, and their
results returned via status callbacks.
"""

from datetime import datetime
from enum import Enum
from typing import Optional
import uuid


class Status(Enum):
    """Lifecycle states a compute task can be in."""

    QUEUED    = "queued"
    RUNNING   = "running"
    COMPLETED = "completed"
    FAILED    = "failed"


class DataMode(Enum):
    """Describes how input data is provided to the compute task."""

    INLINE = "inline"   # data uploaded as a file attachment
    SQL    = "sql"      # data fetched from the live DB via a SQL query


class Compute:
    """Represents a single ML compute task submitted to the queue."""

    def __init__(
        self,
        id: uuid.UUID,
        code_path: str,
        data_mode: DataMode,
        requirements_path: str,
        data_path: Optional[str] = None,
        sql_query: Optional[str] = None,
        status: Status = Status.QUEUED,
        entry_point: Optional[str] = "main",
        created_at: Optional[datetime] = None,
        started_at: Optional[datetime] = None,
        finished_at: Optional[datetime] = None,
        worker_id: Optional[str] = None,
        timeout: int = 3600,
    ):
        """Initialise a Compute task.

        Parameters
        ----------
        id:                 UUID assigned at submission time.
        code_path:          Path to the uploaded Python script on disk.
        data_mode:          INLINE (file) or SQL (query against travel.db).
        requirements_path:  Path to the uploaded requirements.txt.
        data_path:          Path to the optional inline data file.
        sql_query:          SQL string used when data_mode == SQL.
        status:             Initial lifecycle status (default QUEUED).
        entry_point:        Name of the function to call in code_path (default 'main').
        created_at:         Submission timestamp; defaults to now.
        started_at:         Set when a worker picks up the task.
        finished_at:        Set when the task completes or fails.
        worker_id:          Identifier of the worker that ran the task.
        timeout:            Max execution time in seconds (default 3600).
        """
        self.created_at  = created_at or datetime.now()
        self.started_at  = started_at
        self.finished_at = finished_at
        self.worker_id   = worker_id

        self.id                = id
        self.code_path         = code_path
        self.requirements_path = requirements_path
        self.data_mode         = data_mode
        self.status            = status
        self.data_path         = data_path
        self.sql_query         = sql_query
        self.entry_point       = entry_point
        self.timeout           = timeout
