import logging
from fastapi import Depends, APIRouter

from auth import require_upload_token
from compute.ssh import get_last_wol, is_pc_active, shutdown_pc, wake_pc


router = APIRouter()
logger = logging.getLogger(__name__)


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
