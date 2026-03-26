"""
upload/endpoints.py
~~~~~~~~~~~~~~~~~~~
Aggregates the three upload sub-routers (location, transaction, health) under
the /upload prefix.  Each domain manages its own authentication and validation.
"""
from fastapi import APIRouter  # type: ignore
import logging

from upload.location.endpoints import router as location_router
from upload.transaction.endpoints import router as transaction_router
from upload.health.endpoints import router as health_router

router = APIRouter()
logger = logging.getLogger(__name__)

router.include_router(location_router, prefix="/location")
router.include_router(transaction_router, prefix="/transaction")
router.include_router(health_router, prefix="/health")