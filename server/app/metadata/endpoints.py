from datetime import datetime, timezone
import json
import logging
from typing import Any
from config.editable import get_editable
from fastapi import APIRouter, Header, Query, HTTPException  # type: ignore
from fastapi.responses import Response  # type: ignore

from auth import check_auth
from database.exchange.util import get_api_usage
from metadata.util import get_db_stats, get_fx_latest_date, get_last_uploads, get_pending_digest_count, get_uptime, read_last_lines_efficient
from config.general import LOG_FILE, OVERRIDES_PATH
from pydantic import BaseModel # type: ignore


router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/logs")
async def get_logs(lines: int = Query(200, ge=1, le=1000), authorization: str = Header(None)):
    """
    Return the last `lines` lines of the main server logs.
    - `lines`: number of lines to return, default 200, min 1, max 1000
    """
    check_auth(authorization)

    logs = read_last_lines_efficient(LOG_FILE, n=lines)
    return Response(content=logs, media_type="text/plain")

@router.get("/status")
async def get_status():
    return {
        "uptime": get_uptime(),
        "db": get_db_stats(),
        "last_upload": get_last_uploads(),
        "fx_latest_date": get_fx_latest_date(),
        "fx_api_usage": get_api_usage("exchangerate.host"),
        "pending_digest_records": get_pending_digest_count(),
        "generated_at": datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
    }

class ConfigUpdate(BaseModel):
    key: str
    value: Any

@router.get("/config")
async def get_config(authorization: str = Header(None)):
    check_auth(authorization)
    editable = get_editable()
    # Load current overrides to show what's been overridden
    overrides = {}
    if OVERRIDES_PATH.exists():
        try:
            with open(OVERRIDES_PATH) as f:
                overrides = json.load(f)
        except Exception:
            pass
    result = {}
    for key, entry in editable.items():
        result[key] = {
            "value":       entry["value"],
            "default":     entry["default"],
            "overridden":  key in overrides,
            "description": entry["description"],
            "group":       entry["group"],
            "type":        entry["type"],
            "module":      entry["module"],
        }
    return result


@router.post("/config")
async def update_config(update: ConfigUpdate, authorization: str = Header(None)):
    check_auth(authorization)
    editable = get_editable()
    if update.key not in editable:
        raise HTTPException(status_code=400, detail=f"Key '{update.key}' is not editable")

    # Load existing overrides
    overrides = {}
    if OVERRIDES_PATH.exists():
        try:
            with open(OVERRIDES_PATH) as f:
                overrides = json.load(f)
        except Exception:
            pass

    old_value = overrides.get(update.key, editable[update.key]["default"])
    default_value = editable[update.key]["default"]
    overrides[update.key] = update.value

    # Write atomically
    tmp = OVERRIDES_PATH.with_suffix(".tmp")
    with open(tmp, "w") as f:
        json.dump(overrides, f, indent=2, default=str)
    tmp.replace(OVERRIDES_PATH)

    logger.info(f"Config updated: {update.key} | {old_value!r} → {update.value!r} (default={default_value!r})")
    return {"message": f"'{update.key}' saved. Restart the server to apply."}


@router.delete("/config/{key}")
async def reset_config(key: str, authorization: str = Header(None)):
    check_auth(authorization)
    if not OVERRIDES_PATH.exists():
        raise HTTPException(status_code=404, detail="No overrides file found")
    with open(OVERRIDES_PATH) as f:
        overrides = json.load(f)
    if key not in overrides:
        raise HTTPException(status_code=404, detail=f"Key '{key}' has no override")
    del overrides[key]
    tmp = OVERRIDES_PATH.with_suffix(".tmp")
    with open(tmp, "w") as f:
        json.dump(overrides, f, indent=2, default=str)
    tmp.replace(OVERRIDES_PATH)
    logger.info(f"Config reset to default: {key}")
    return {"message": f"'{key}' reset to default. Restart the server to apply."}