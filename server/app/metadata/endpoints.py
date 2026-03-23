from datetime import datetime, timezone
import json
import logging
from typing import Any
from config.editable import get_editable, get_value
from fastapi import APIRouter, Header, Query, HTTPException, Body  # type: ignore
from fastapi.responses import Response  # type: ignore

from auth import check_auth
from database.exchange.util import get_api_usage
from metadata.util import get_db_stats, get_fx_latest_date, get_last_uploads, get_local_backups, get_pending_digest_count, get_remote_backups, get_uptime, read_last_lines_efficient
from config.general import LOG_FILE, OVERRIDES_PATH, STALE_DAYS
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
    """Return a live system status snapshot (uptime, DB stats, last uploads, FX info)."""
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


def _coerce_value(value: Any, type_str: str) -> Any:
    """Coerce and validate a config value against its registered type."""
    try:
        if type_str == "bool":
            if isinstance(value, bool):
                return value
            return str(value).lower() in ("true", "1", "yes")
        if type_str == "int":
            return int(value)
        if type_str == "float":
            return float(value)
        if type_str == "str":
            return str(value)
        if type_str == "dict":
            if not isinstance(value, dict):
                raise ValueError(f"expected dict, got {type(value).__name__}")
            return value
        if type_str.startswith("list"):
            if not isinstance(value, list):
                raise ValueError(f"expected list, got {type(value).__name__}")
            return value
        # datetime and unknown types — pass through unchanged
        return value
    except (ValueError, TypeError) as e:
        raise ValueError(f"Cannot coerce value to {type_str}: {e}")


@router.get("/config")
async def get_config(authorization: str = Header(None)):
    """Return all registered editable config values, including override status."""
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
    """Persist a config override to config_overrides.json (requires server restart to apply)."""
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
    type_str = editable[update.key]["type"]
    try:
        coerced = _coerce_value(update.value, type_str)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    overrides[update.key] = coerced

    # Write atomically
    tmp = OVERRIDES_PATH.with_suffix(".tmp")
    with open(tmp, "w") as f:
        json.dump(overrides, f, indent=2, default=str)
    tmp.replace(OVERRIDES_PATH)

    logger.info(f"Config updated: {update.key} | {old_value!r} → {coerced!r} (default={default_value!r})")
    return {"message": f"'{update.key}' saved. Restart the server to apply."}


@router.delete("/config/{key}")
async def reset_config(key: str, authorization: str = Header(None)):
    """Remove a config override for key, restoring its default value on next restart."""
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

@router.get("/backups")
async def get_backups(authorization: str = Header(None)):
    """Return latest local and remote (Cloudflare R2) backup info for all data types."""
    check_auth(authorization)
    return {
        "local":  get_local_backups(),
        "remote": get_remote_backups(),
        "stale_days": get_value("STALE_DAYS", STALE_DAYS)
    }

class GapRequest(BaseModel):
    content: str

@router.post("/describe_gap")
async def describe_gap(body: GapRequest,
                       authorization: str = Header(None),
                       start_time: datetime = Header(None),
                       end_time: datetime = Header(None)):
    check_auth(authorization)

    return {
        "start_time": start_time,
        "end_time": end_time,
        "content": body.content
    }