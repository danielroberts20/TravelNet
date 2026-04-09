from datetime import datetime
import logging
from typing import Any, Optional
from fastapi import Header, UploadFile, File, Form, HTTPException, APIRouter, BackgroundTasks, Depends  # type: ignore

from auth import require_upload_token
from compute.models import DataMode, Status
from database.compute.table import table as compute_table
from compute.storage import store_compute
from compute.ssh import get_last_wol, is_pc_active, shutdown_pc, ssh_run, wake_pc


router = APIRouter()
logger = logging.getLogger(__name__)
compute_queue = []


@router.post("/submit", dependencies=[Depends(require_upload_token)])
async def submit_compute(
    code: UploadFile = File(...),
    requirements: UploadFile = File(...),
    data_mode: DataMode = Form(...),
    data_file: Optional[UploadFile] = File(None),
    sql_query: Optional[str] = Form(None),
    entry_point: Optional[str] = Form("main"),
    timeout: int = Form(3600),
):
    """Accept a user-submitted ML compute task (code, requirements, optional data), queue it for execution."""
    if data_mode == DataMode.SQL and not sql_query:
        raise HTTPException(status_code=400, detail="SQL mode requires sql_query")

    if data_mode == DataMode.INLINE and not data_file:
        raise HTTPException(status_code=400, detail="INLINE mode requires data_file")

    if timeout > 14400:
        raise HTTPException(status_code=400, detail="Timeout too large")

    item = store_compute(
        code=code,
        requirements=requirements,
        data_file=data_file,
        data_mode=data_mode,
        sql_query=sql_query,
        entry_point=entry_point,
        timeout=timeout,
    )

    compute_table.insert(item)

    return {
        "status": "success",
        "compute_id": item.id
    }


@router.get("/list", dependencies=[Depends(require_upload_token)])
async def list_compute():
    """Return all in-memory QUEUED compute tasks (legacy endpoint; DB-backed equivalent is /next)."""
    return {
        "compute": [vars(item) for item in compute_queue if item.status == Status.QUEUED]
    }


@router.get("/next-legacy", dependencies=[Depends(require_upload_token)])
async def get_next_compute_legacy():
    """Return the first QUEUED in-memory compute task and mark it RUNNING (legacy in-memory version)."""
    queued = [item for item in compute_queue if item.status == Status.QUEUED]
    if queued:
        selected = queued[0]
        selected.status = Status.RUNNING
        return {"compute": vars(selected)}
    else:
        return {"compute": None}


@router.post("/gpu-info", dependencies=[Depends(require_upload_token)])
async def gpu_info(data: dict[str, Any], background_tasks: BackgroundTasks):
    """Accept a GPU telemetry report from a worker node and log it."""
    logger.info(f"Received GPU info: {data}")
    return {
        "status": "success",
    }


@router.get("/next", dependencies=[Depends(require_upload_token)])
async def next_compute():
    """Return the next QUEUED compute task from the DB (DB-backed version of next-legacy)."""
    item = compute_table.get_next_queued()
    if item:
        return {"compute": vars(item)}
    else:
        return {"compute": None}


@router.get("/start-next", dependencies=[Depends(require_upload_token)])
async def start_next_compute():
    """Fetch the next QUEUED compute task, mark it RUNNING, persist the status change, and return it."""
    item = compute_table.get_next_queued()
    if item:
        item.status = Status.RUNNING
        item.started_at = datetime.now()
        compute_table.update(item.id, item)
        return {"compute": vars(item)}
    else:
        return {"compute": None}

@router.post("/wake", dependencies=[Depends(require_upload_token)])
def wake():
    wake_pc()
    logger.info("Wake command sent to PC")
    return {"status": "wake command sent"}

@router.get("/pc-status", dependencies=[Depends(require_upload_token)])
def pc_status():
    return {"active": is_pc_active()}

@router.get("/last-wol", dependencies=[Depends(require_upload_token)])
def last_wol():
    return {"timestamp": get_last_wol()}

@router.post("/shutdown", dependencies=[Depends(require_upload_token)])
def shutdown():
    shutdown_pc()
    logger.info("Shutdown command sent to PC")
    return {"status": "shutdown command sent"}

