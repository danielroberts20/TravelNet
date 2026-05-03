import logging

from auth import require_watchdog_token
from pydantic import BaseModel #type: ignore
from fastapi import APIRouter, Depends, BackgroundTasks  # type: ignore
from database.watchdog.table import table as watchdog_table, WatchdogHeartbeatRecord

router = APIRouter()
logger = logging.getLogger(__name__)

class HeartbeatPayload(BaseModel):
    timestamp: str
    internet_ok: bool
    tailscale_ok: bool
    api_ok: bool
    prefect_ok: bool
    consecutive_failures: int = 0

def insert_heartbeat(payload: HeartbeatPayload) -> None:
    watchdog_table.insert(WatchdogHeartbeatRecord(
        timestamp=payload.timestamp,
        internet_ok=payload.internet_ok,
        tailscale_ok=payload.tailscale_ok,
        api_ok=payload.api_ok,
        prefect_ok=payload.prefect_ok,
        consecutive_failures=payload.consecutive_failures))
    

@router.post("/heartbeat", dependencies=[Depends(require_watchdog_token)])
async def heartbeat(
    payload: HeartbeatPayload,
    background_tasks: BackgroundTasks):
    background_tasks.add_task(insert_heartbeat, payload)
    return {"status": "ok"}