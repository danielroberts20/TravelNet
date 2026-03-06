from datetime import datetime
import logging
from typing import Any, Optional
import uuid
from fastapi import Header, UploadFile, File, Form, HTTPException, APIRouter, BackgroundTasks  # type: ignore

from auth import check_auth
from jobs.models import DataMode, Status
from database.job.table import get_next_queued_job, insert_job, update_job
from jobs.utils import store_job


router = APIRouter()
logger = logging.getLogger(__name__)
jobs = []

@router.post("/submit-job")
async def submit_job(
    authorization: str = Header(None),

    code: UploadFile = File(...),
    requirements: UploadFile = File(...),

    data_mode: DataMode = Form(...),

    data_file: Optional[UploadFile] = File(None),
    sql_query: Optional[str] = Form(None),
    entry_point: Optional[str] = Form("main"),
    timeout: int = Form(3600),
):
    check_auth(authorization)

    # -------- Validation --------

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

    # Store in memory (replace with DB later)
    insert_job(job)

    return {
        "status": "success",
        "job_id": job.id
    }

@router.get("/get-jobs")
async def get_jobs(authorization: str = Header(None)):
    check_auth(authorization)

    return {
        "jobs": [vars(job) for job in jobs if job.status == Status.QUEUED]
    }

@router.get("/get-next-job")
async def get_next_job(authorization: str = Header(None)):
    check_auth(authorization)

    # Return the first job in the list (in a real app, this would be more sophisticated)
    queued_jobs = [job for job in jobs if job.status == Status.QUEUED]
    if queued_jobs:
        selected_job = queued_jobs[0]
        selected_job.status = Status.RUNNING
        return {"job": vars(selected_job)}
    else:
        return {"job": None}

@router.post("/update-job-status")
async def update_job_status(job_id: str, status: Status, authorization: str = Header(None)):
    check_auth(authorization)

    update_job_status(job_id, status)
    
    raise HTTPException(status_code=404, detail="Job not found")

@router.post("/gpu-info")
async def gpu_info(data: dict[str, Any],
                background_tasks: BackgroundTasks,
                authorization: str = Header(...)):
    
        check_auth(authorization)
        logger.info(f"Received GPU info: {data}")
        return {
            "status": "success",
        }

@router.get("/next")
async def next_job(authorization: str = Header(...)):
    check_auth(authorization)
    job = get_next_queued_job()
    if job:
        return {"job": vars(job)}
    else:
        return {"job": None}
    
@router.get("/start-next-job")
async def start_next_job(authorization: str = Header(...)):
    check_auth(authorization)
    job = get_next_queued_job()
    if job:
        job.status = Status.RUNNING
        job.started_at = datetime.now()
        update_job(job.id, job)
        return {"job": vars(job)}
    else:
        return {"job": None}
