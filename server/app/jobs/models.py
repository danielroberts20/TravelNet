from datetime import datetime
from enum import Enum
from typing import Optional
import uuid

class Status(Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"

class DataMode(Enum):
    INLINE = "inline"
    SQL = "sql"

class Job:
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
        self.created_at = created_at or datetime.now()
        self.started_at = started_at
        self.finished_at = finished_at
        self.worker_id = worker_id

        self.id = id
        self.code_path = code_path
        self.requirements_path = requirements_path
        self.data_mode = data_mode
        self.status = status
        self.data_path = data_path
        self.sql_query = sql_query
        self.entry_point = entry_point
        self.timeout = timeout
        