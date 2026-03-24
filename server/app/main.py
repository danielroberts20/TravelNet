from datetime import datetime, timezone
import logging
from fastapi import FastAPI  # type: ignore

from config.logging import configure_logging
from config.editable import load_overrides
from database.integration import init_db
from database.util import rebuild_db

configure_logging()
load_overrides()

from jobs.endpoints import router as jobs_router
from upload.endpoints import router as uploads_router
from database.endpoints import router as db_router
from metadata.endpoints import router as metadata_router
from notifications import send_notification
import config.runtime # Records timestamp that docker container started


logger = logging.getLogger(__name__)

init_db()
# rebuild_db("fx_rates")
logger.info("Database initialized.")

app = FastAPI(title="TravelNet API", version="1.0.1")

app.include_router(jobs_router, prefix="/jobs")
app.include_router(uploads_router, prefix="/upload")
app.include_router(db_router, prefix="/database")
app.include_router(metadata_router, prefix="/metadata")

@app.on_event("startup")
async def on_startup():
    from config.general import COUNTRY_DEPARTURE_DATES
    from scheduled_tasks.departure_backup import schedule_departure_backups
    schedule_departure_backups(COUNTRY_DEPARTURE_DATES)
    send_notification(
        title="TravelNet",
        body="✅ Server online",
        use_prefix=False
    )