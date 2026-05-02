from datetime import datetime, timezone
import json
import logging
from typing import Any
from database.location.geocoding import get_place_id
from database.connection import get_conn
from config.editable import get_editable, get_value, coerce_value
from fastapi import APIRouter, Query, HTTPException, Body, Depends  # type: ignore
from fastapi.responses import Response  # type: ignore

from auth import require_upload_token
from database.exchange.fx import get_api_usage
from database.location.gap_annotations.table import table as gap_annotations_table, GapAnnotationRecord
from metadata.system import get_db_stats, get_pending_digest_count, get_uptime, read_last_lines_efficient
from metadata.uploads import get_fx_latest_date, get_last_uploads
from metadata.backups import get_local_backups, get_remote_backups
from config.general import GAP_ANNOTATION_TOLERANCE_MINUTES, LOG_FILE, OVERRIDES_PATH, STALE_DAYS
from notifications import send_notification
from pydantic import BaseModel  # type: ignore
from datetime import datetime
from zoneinfo import ZoneInfo

from triggers.location_change import label_place  # type: ignore
from scheduled_tasks.update_timezone import update_timezones_flow


router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/logs", dependencies=[Depends(require_upload_token)])
async def get_logs(lines: int = Query(200, ge=1, le=1000)):
    """Return the last `lines` lines of the main server log file."""
    logs = read_last_lines_efficient(LOG_FILE, n=lines)
    return Response(content=logs, media_type="text/plain")


@router.get("/watchdog", dependencies=[Depends(require_upload_token)])
async def get_watchdog():
    return {
        "status": "ok",
        "uptime": get_uptime()
    }


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
    """Record a known gap in location/health data with a human-readable description."""
    start_ts = int(body.start_time.timestamp())
    end_ts = int(body.end_time.timestamp())

    if end_ts <= start_ts:
        raise HTTPException(status_code=400, detail="end_time must be after start_time")

    annotation_id = gap_annotations_table.insert(GapAnnotationRecord(
        start_ts=start_ts, end_ts=end_ts, reason="manual", description=body.description,
    ))
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
    return {"annotations": gap_annotations_table.list_annotations()}


# ---------------------------------------------------------------------------
# Deployment timezone conversion
# ---------------------------------------------------------------------------


def _tz_offset(iana_tz: str) -> str:
    """Return the current UTC offset for an IANA timezone, e.g. '+10:00'."""
    offset = datetime.now(ZoneInfo(iana_tz)).utcoffset()
    total_mins = int(offset.total_seconds() // 60)
    sign = "+" if total_mins >= 0 else "-"
    h, m = divmod(abs(total_mins), 60)
    return f"{sign}{h:02d}:{m:02d}"

class DeploymentTzRequest(BaseModel):
    timezone: str
    latitude: float | None = None
    longitude: float | None = None


@router.post("/deployment_tz", dependencies=[Depends(require_upload_token)])
async def update_deployment_tz(body: DeploymentTzRequest):
    """Update all Prefect deployment cron schedules to fire at the same wall-clock time
    in the given timezone. No-ops if the timezone hasn't changed since the last update."""
    with get_conn(read_only=True) as conn:
        row = conn.execute("""
            SELECT to_tz FROM transition_timezone
            ORDER BY transitioned_at DESC
            LIMIT 1
        """).fetchone()
        current_tz = row["to_tz"] if row else None

    if current_tz == body.timezone:
        logger.info("Timezone unchanged (%s), skipping update.", body.timezone)
        return {"status": "unchanged", "timezone": body.timezone}

    await update_timezones_flow(timezone=body.timezone)

    with get_conn() as conn:
        place_id = None
        if body.latitude is not None and body.longitude is not None:
            place_id = get_place_id(body.latitude, body.longitude, conn=conn)

        conn.execute("""
            INSERT INTO transition_timezone (transitioned_at, from_tz, to_tz, from_offset, to_offset, place_id)
            VALUES (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'), ?, ?, ?, ?, ?)
        """, (
            current_tz,
            body.timezone,
            _tz_offset(current_tz) if current_tz else None,
            _tz_offset(body.timezone),
            place_id,
        ))

    send_notification(
        title="Timezone Changed",
        body=f"Schedules updated: {current_tz or 'unknown'} → {body.timezone}",
        time_sensitive=False,
    )
    logger.info("Timezone changed: %s → %s", current_tz, body.timezone)
    return {"status": "updated", "from": current_tz, "to": body.timezone}

@router.get("/widget_status")
async def widget_status():
    return {
        "value": 0.72,
        "label": "72%",
        "symbol": "externaldrive.fill",
        "color": "green"
    }

@router.post("/label-place")
async def label_location(body):
    label = body.get("label", {})
    place_id = body.get("id", {})

    if place_id:
        logger.info(f"Received label '{label}' for location ID {place_id}")
        if label_place(place_id, label):
            return {"status": "success"}
        else:
            logger.warning(f"Place ID {place_id} not in known_places table")
            raise HTTPException(status_code=404, detail="Place ID not found")
    else:
        logger.warning("No location ID provided in label-location request")
        raise HTTPException(status_code=400, detail="Place ID is required")
