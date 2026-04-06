from datetime import datetime, timezone
import logging
from fastapi import FastAPI  # type: ignore

from config.logging import configure_logging
from config.editable import load_overrides
from database.setup import init_db
from database.connection import rebuild_db
from compute.util import ssh_run
from compute.router import wake
from compute.util import wake_pc
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from starlette.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

configure_logging()
load_overrides()

from compute.router import router as compute_router
from upload.router import router as uploads_router
from database.admin import router as db_router
from metadata.router import router as metadata_router
from public.router import router as public_router
from notifications import send_notification
from config.general import PUBLIC_ALLOWED_PREFIXES
import config.runtime # Records timestamp that docker container started


logger = logging.getLogger(__name__)

init_db()
# rebuild_db("fx_rates")
logger.info("Database initialized.")

app = FastAPI(title="TravelNet API", version="1.0.1")

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

class PublicPathFilterMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        host = request.headers.get("host", "")
        if "api.travelnet.dev" in host:
            if not any(request.url.path.startswith(p) for p in PUBLIC_ALLOWED_PREFIXES):
                return JSONResponse({"detail": "Forbidden"}, status_code=403)
        return await call_next(request)

app.add_middleware(PublicPathFilterMiddleware)

app.include_router(compute_router, prefix="/compute")
app.include_router(uploads_router, prefix="/upload")
app.include_router(db_router, prefix="/database")
app.include_router(metadata_router, prefix="/metadata")
app.include_router(public_router, prefix="/public")

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