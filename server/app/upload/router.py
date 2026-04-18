"""
upload/router.py
~~~~~~~~~~~~~~~~
Aggregates the three upload sub-routers (location, transaction, health) under
the /upload prefix.  Each domain manages its own authentication and validation.
"""
from fastapi import APIRouter  # type: ignore
import logging

from upload.location.router import router as location_router
from upload.transaction.router import router as transaction_router
from upload.health.router import router as health_router
from upload.journal.router import router as journal_router
from upload.places.router import router as places_router
from upload.flight.router import router as flight_router
from upload.watchdog.router import router as watchdog_router
from upload.photos.router import router as photos_router

router = APIRouter()
logger = logging.getLogger(__name__)

router.include_router(location_router, prefix="/location")
router.include_router(transaction_router, prefix="/transaction")
router.include_router(health_router, prefix="/health")
router.include_router(journal_router, prefix="/journal")
router.include_router(places_router, prefix="/places")
router.include_router(flight_router, prefix="/flight")
router.include_router(watchdog_router, prefix="/watchdog")
router.include_router(photos_router, prefix="/photos")
