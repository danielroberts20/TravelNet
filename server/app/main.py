import logging
import os
from fastapi import FastAPI, BackgroundTasks, Header  # type: ignore
from fastapi.responses import FileResponse  # type: ignore

from config.logging import configure_logging
from database.util import backup_db
from database.integration import init_db
from auth import check_auth
from jobs.endpoints import router as jobs_router
from uploads.endpoints import router as uploads_router


configure_logging()
logger = logging.getLogger(__name__)

app = FastAPI(title="TravelNet API", version="1.0.0")

app.include_router(jobs_router, prefix="/jobs")
app.include_router(uploads_router, prefix="/uploads")

init_db()

@app.get("/database")
async def query(background_tasks: BackgroundTasks, authorization: str = Header(None)):
    check_auth(authorization)

    backup_path = backup_db()
    background_tasks.add_task(os.remove, backup_path)

    return FileResponse(
        path=backup_path,
        filename=backup_path.name,
        media_type="application/octet-stream"
    )