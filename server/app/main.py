"""
main.py
~~~~~~~
TravelNet API entry point.

Startup order:
  1. configure_logging() + load_overrides() run at module level so that all
     subsequently imported modules inherit the correct log levels and config
     overrides.
  2. The lifespan context manager handles init_db(), departure-backup
     scheduling, and the startup notification — all of which need the
     application to be fully assembled before they run.
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI  # type: ignore
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from starlette.middleware.cors import CORSMiddleware

from config.logging import configure_logging
from config.editable import load_overrides

# Must run before any module that uses logging or reads editable config
configure_logging()
load_overrides()

import config.runtime  # records the timestamp the container started

from compute.ssh import ssh_run, wake_pc
from compute.router import router as compute_router, wake
from database.setup import init_db
from middleware import PublicPathFilterMiddleware
from notifications import send_notification
from upload.router import router as uploads_router
from database.admin import router as db_router
from metadata.router import router as metadata_router
from public.router import router as public_router

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup: initialise DB, schedule departure backups, send online notification."""
    init_db()
    logger.info("Database initialized.")

    from config.general import COUNTRY_DEPARTURE_DATES
    from scheduled_tasks.departure_backup import schedule_departure_backups
    schedule_departure_backups(COUNTRY_DEPARTURE_DATES)

    send_notification(title="TravelNet", body="✅ Server online", use_prefix=False)

    yield

    send_notification(title="TravelNet", body="❌ Server offline", use_prefix=False)


app = FastAPI(title="TravelNet API", version="1.0.1", lifespan=lifespan)

# --- Rate limiting ---
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# --- CORS: public endpoint only ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://travelnet.dev", "https://travelnet.pages.dev", "https://www.travelnet.dev"],
    allow_methods=["GET"],
    allow_headers=[],
)

app.add_middleware(PublicPathFilterMiddleware)

app.include_router(compute_router, prefix="/compute")
app.include_router(uploads_router, prefix="/upload")
app.include_router(db_router, prefix="/database")
app.include_router(metadata_router, prefix="/metadata")
app.include_router(public_router, prefix="/public")
