from datetime import datetime
import logging
from typing import Any, Optional
import uuid
from fastapi import Header, UploadFile, File, Form, HTTPException, APIRouter, BackgroundTasks, Depends  # type: ignore

from auth import require_upload_token
from jobs.models import DataMode, Status
from database.job.table import get_next_queued_job, insert_job, update_job
from jobs.utils import store_job


router = APIRouter()
logger = logging.getLogger(__name__)
jobs = []


@router.post("/submit-job", dependencies=[Depends(require_upload_token)])
async def submit_job(
    code: UploadFile = File(...),
    requirements: UploadFile = File(...),
    data_mode: DataMode = Form(...),
    data_file: Optional[UploadFile] = File(None),
    sql_query: Optional[str] = Form(None),
    entry_point: Optional[str] = Form("main"),
    timeout: int = Form(3600),
):
    """Accept a user-submitted ML job (code, requirements, optional data), queue it for execution.

    Returns the assigned job_id on success.  Validates that:
      - SQL mode has a sql_query
      - INLINE mode has a data_file
      - timeout does not exceed 4 hours (14400 s)
    """
    if data_mode == DataMode.SQL and not sql_query:
        raise HTTPException(status_code=400, detail="SQL mode requires sql_query")

    if data_mode == DataMode.INLINE and not data_file:
        raise HTTPException(status_code=400, detail="INLINE mode requires data_file")

    if timeout > 14400:
        raise HTTPException(status_code=400, detail="Timeout too large")

    job = store_job(
        job_id=str(uuid.uuid4()),
        code=code,
        requirements=requirements,
        data_file=data_file,
        data_mode=data_mode,
        sql_query=sql_query,
        entry_point=entry_point,
        timeout=timeout,
    )

    insert_job(job)

    return {
        "status": "success",
        "job_id": job.id
    }


@router.get("/get-jobs", dependencies=[Depends(require_upload_token)])
async def get_jobs():
    """Return all in-memory QUEUED jobs (legacy endpoint; DB-backed equivalent is /next)."""
    return {
        "jobs": [vars(job) for job in jobs if job.status == Status.QUEUED]
    }


@router.get("/get-next-job", dependencies=[Depends(require_upload_token)])
async def get_next_job():
    """Return the first QUEUED in-memory job and mark it RUNNING (legacy in-memory version)."""
    queued_jobs = [job for job in jobs if job.status == Status.QUEUED]
    if queued_jobs:
        selected_job = queued_jobs[0]
        selected_job.status = Status.RUNNING
        return {"job": vars(selected_job)}
    else:
        return {"job": None}


@router.post("/update-job-status", dependencies=[Depends(require_upload_token)])
async def update_job_status(job_id: str, status: Status):
    """Update the status of a job by ID (currently raises 404 — placeholder implementation)."""
    update_job_status(job_id, status)
    raise HTTPException(status_code=404, detail="Job not found")


@router.post("/gpu-info", dependencies=[Depends(require_upload_token)])
async def gpu_info(data: dict[str, Any], background_tasks: BackgroundTasks):
    """Accept a GPU telemetry report from a worker node and log it."""
    logger.info(f"Received GPU info: {data}")
    return {
        "status": "success",
    }


@router.get("/next", dependencies=[Depends(require_upload_token)])
async def next_job():
    """Return the next QUEUED job from the DB (DB-backed version of get-next-job)."""
    job = get_next_queued_job()
    if job:
        return {"job": vars(job)}
    else:
        return {"job": None}


@router.get("/start-next-job", dependencies=[Depends(require_upload_token)])
async def start_next_job():
    """Fetch the next QUEUED job, mark it RUNNING, persist the status change, and return it."""
    job = get_next_queued_job()
    if job:
        job.status = Status.RUNNING
        job.started_at = datetime.now()
        update_job(job.id, job)
        return {"job": vars(job)}
    else:
        return {"job": None}
