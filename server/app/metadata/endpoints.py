from datetime import datetime, timezone
import json
import logging
from typing import Any
from config.editable import get_editable, get_value, coerce_value
from fastapi import APIRouter, Query, HTTPException, Body, Depends  # type: ignore
from fastapi.responses import Response  # type: ignore

from config.auth import require_upload_token
from metadata.crontab_tz import reset_crontab_timezone, update_crontab_timezone
from database.exchange.util import get_api_usage
from database.location.gap_annotations.table import insert_annotation, list_annotations
from metadata.util import get_db_stats, get_fx_latest_date, get_last_uploads, get_local_backups, get_pending_digest_count, get_remote_backups, get_uptime, read_last_lines_efficient
from config.general import GAP_ANNOTATION_TOLERANCE_MINUTES, LOG_FILE, OVERRIDES_PATH, STALE_DAYS
from config.notifications import send_notification
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
        coerced = coerce_value(update.value, type_str)
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


# ---------------------------------------------------------------------------
# Crontab timezone conversion
# ---------------------------------------------------------------------------

class CrontabTzRequest(BaseModel):
    """Request body for re-timing the crontab to a new local timezone."""

    timezone: str


@router.post("/crontab_tz", dependencies=[Depends(require_upload_token)])
async def update_crontab_tz(body: CrontabTzRequest):
    """Re-time all cron jobs so they fire at the same wall-clock time in the given timezone.

    Takes the current cron schedule (written in Europe/London time) and converts
    it so every job fires at the equivalent local time for the supplied timezone.
    For example, a job at 06:00 with timezone='America/New_York' (EST, UTC-5)
    becomes 11:00 Pi time (11:00 GMT = 06:00 EST).

    Accepts IANA names ('America/New_York'), UTC offsets ('+1000'), or
    common abbreviations ('EST', 'JST').

    NOTE: requires the Pi's crontab to be accessible from within the Docker
    container (e.g. via a /var/spool/cron/crontabs volume mount).
    """
    try:
        result = update_crontab_timezone(body.timezone)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))

    if result['skipped']:
        logger.info("Crontab timezone unchanged (%s) — skipped", result['timezone_label'])
        return {
            "timezone":     result['timezone_label'],
            "jobs_changed": 0,
            "details":      [],
            "skipped":      True,
        }

    changed_count = sum(1 for c in result['changes'] if c['changed'])
    send_notification(
        title="Cron Updated",
        body=f"Schedule adjusted to {result['timezone_label']} ({changed_count} job(s) re-timed)",
        time_sensitive=False,
    )
    logger.info(
        "Crontab timezone updated to %s (pi_offset=%+d min, user_offset=%+d min, %d changed)",
        result['timezone_label'],
        result['pi_offset_min'],
        result['user_offset_min'],
        changed_count,
    )

    return {
        "timezone":     result['timezone_label'],
        "jobs_changed": changed_count,
        "details":      result['changes'],
        "skipped":      False,
    }


@router.delete("/crontab_tz", dependencies=[Depends(require_upload_token)])
async def reset_crontab_tz():
    """Restore the crontab to the state it was in before the last timezone conversion.

    The backup is saved automatically each time POST /crontab_tz is called.
    Only available in Docker mode (requires CRONTAB_FILE to be set).
    """
    try:
        content = reset_crontab_timezone()
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))

    line_count = sum(1 for l in content.splitlines() if l.strip() and not l.startswith('#'))
    send_notification(
        title="Cron Reset",
        body=f"Crontab restored to pre-conversion backup ({line_count} job(s))",
        time_sensitive=False,
    )
    logger.info("Crontab reset to pre-conversion backup")
    return {"message": "Crontab restored from backup."}

@router.get("/widget_status")
async def widget_status():
    return {
        "value": 0.72,
        "label": "72%",
        "symbol": "externaldrive.fill",
        "color": "green"
    } 