"""
internal/router.py
~~~~~~~~~~~~~~~~
Internal endpoint.
"""

import logging
from fastapi import APIRouter  # type: ignore
from pydantic import BaseModel

logger = logging.getLogger(__name__)

router = APIRouter()

class LogRequest(BaseModel):
    """
    Custom request class for logging endpoint to capture request body and other details.
    """
    message: str

@router.post("/log/info")
async def log_info(request: LogRequest):
    logger.info(f"{request.message}")

@router.post("/log/warning")
async def log_warning(request: LogRequest):
    logger.warning(f"{request.message}")

@router.post("/log/important")
async def log_important(request: LogRequest):
    logger.important(f"{request.message}")

@router.post("/log/error")
async def log_error(request: LogRequest):
    logger.error(f"{request.message}")
