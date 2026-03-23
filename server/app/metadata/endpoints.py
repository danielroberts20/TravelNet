from datetime import datetime, timezone
import json
import logging
from typing import Any
from config.editable import get_editable, get_value
from fastapi import APIRouter, Query, HTTPException, Body, Depends  # type: ignore
from fastapi.responses import Response  # type: ignore

from auth import require_upload_token
from database.exchange.util import get_api_usage
from database.location.gap_annotations.table import insert_annotation, list_annotations
from metadata.util import get_db_stats, get_fx_latest_date, get_last_uploads, get_local_backups, get_pending_digest_count, get_remote_backups, get_uptime, read_last_lines_efficient
from config.general import GAP_ANNOTATION_TOLERANCE_MINUTES, LOG_FILE, OVERRIDES_PATH, STALE_DAYS
from pydantic import BaseModel  # type: ignore


router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/logs", dependencies=[Depends(require_upload_token)])
async def get_logs(lines: int = Query(200, ge=1, le=1000)):
    """Return the last `lines` lines of the main server log file.

    - `lines`: number of lines to return (default 200, min 1, max 1000)
    """
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
    """Request body for updating a single editable config key."""

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


@router.get("/config", dependencies=[Depends(require_upload_token)])
async def get_config():
    """Return all registered editable config values, including override status."""
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


@router.post("/config", dependencies=[Depends(require_upload_token)])
async def update_config(update: ConfigUpdate):
    """Persist a config override to config_overrides.json (requires server restart to apply)."""
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


@router.delete("/config/{key}", dependencies=[Depends(require_upload_token)])
async def reset_config(key: str):
    """Remove a config override for key, restoring its default value on next restart."""
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


@router.get("/backups", dependencies=[Depends(require_upload_token)])
async def get_backups():
    """Return latest local and remote (Cloudflare R2) backup info for all data types."""
    return {
        "local":  get_local_backups(),
        "remote": get_remote_backups(),
        "stale_days": get_value("STALE_DAYS", STALE_DAYS)
    }


# ---------------------------------------------------------------------------
# Gap annotations
# ---------------------------------------------------------------------------

class GapAnnotationRequest(BaseModel):
    """Request body for annotating a known gap in location or health data."""

    start_time: datetime
    end_time: datetime
    description: str


@router.post("/annotate_gap", dependencies=[Depends(require_upload_token)])
async def annotate_gap(body: GapAnnotationRequest):
    """Record a known gap in location/health data with a human-readable description.

    Gaps are stored as Unix timestamp ranges.  When gap-detection logic later
    encounters a gap it can call is_gap_covered() to check whether a matching
    annotation exists within the configured tolerance window
    (GAP_ANNOTATION_TOLERANCE_MINUTES).

    Example use case: phone was in for battery replacement from ~10:00 to ~10:45.
    """
    start_ts = int(body.start_time.timestamp())
    end_ts = int(body.end_time.timestamp())

    if end_ts <= start_ts:
        raise HTTPException(status_code=400, detail="end_time must be after start_time")

    annotation_id = insert_annotation(start_ts, end_ts, body.description)
    logger.info(
        "Gap annotation created: id=%d, %s → %s (%s)",
        annotation_id,
        body.start_time.isoformat(),
        body.end_time.isoformat(),
        body.description,
    )

    tolerance = get_value("GAP_ANNOTATION_TOLERANCE_MINUTES", GAP_ANNOTATION_TOLERANCE_MINUTES)
    return {
        "id": annotation_id,
        "start_ts": start_ts,
        "end_ts": end_ts,
        "start_time": body.start_time.isoformat(),
        "end_time": body.end_time.isoformat(),
        "description": body.description,
        "tolerance_minutes": tolerance,
    }


@router.get("/annotations", dependencies=[Depends(require_upload_token)])
async def get_annotations():
    """Return all recorded gap annotations ordered by start time."""
    return {"annotations": list_annotations()}
