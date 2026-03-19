from datetime import datetime, timezone
import logging
from fastapi import FastAPI  # type: ignore

from config.logging import configure_logging
from database.integration import init_db
from database.util import backup_db, rebuild_db

from jobs.endpoints import router as jobs_router
from upload.endpoints import router as uploads_router
from database.endpoints import router as db_router
from metadata.endpoints import router as metadata_router
import config.runtime # Records timestamp that docker container started


configure_logging()
logger = logging.getLogger(__name__)

init_db()
# rebuild_db("fx_rates")
backup_db(include_timestamp=True)
logger.info("Database initialized and backup created.")

app = FastAPI(title="TravelNet API", version="1.0.1")

app.include_router(jobs_router, prefix="/jobs")
app.include_router(uploads_router, prefix="/upload")
app.include_router(db_router, prefix="/database")
app.include_router(metadata_router, prefix="/metadata")