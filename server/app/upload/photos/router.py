import logging
from base64 import b64decode

from auth import require_upload_token
from pydantic import BaseModel
from fastapi import APIRouter, Depends, BackgroundTasks, Request
import json
from database.photos.table import table as photos_table, PhotoMetadataRecord
from database.location.geocoding import get_place_id

router = APIRouter()
logger = logging.getLogger(__name__)


class PhotoItem(BaseModel):
    filename: str | None = None
    file_extension: str | None = None
    file_path: str | None = None
    file_size_bytes: int | None = None
    date_created: str | None = None
    taken_at: str | None = None
    width: int | None = None
    height: int | None = None
    location_lat: float | None = None
    location_lon: float | None = None
    media_type: str | None = None
    photo_type: str | None = None
    is_screenshot: bool = False
    is_screen_recording: bool = False
    is_favourite: bool = False
    duration_s: float | None = None
    camera_make: str | None = None
    camera_model: str | None = None
    album: str | None = None
    exif: dict | None = None


class PhotosPayload(BaseModel):
    photos: list[PhotoItem]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_taken_at(item: PhotoItem) -> str | None:
    """Resolve best available timestamp, normalised to ISO 8601 UTC Z suffix.

    Priority:
      1. EXIF DateTimeOriginal/DateTimeDigitized + OffsetTimeOriginal (most accurate)
      2. date_created from Shortcuts (has tz offset, good for Snapchat)
      3. taken_at from Shortcuts (fallback)
    """
    from datetime import datetime, timezone as dt_timezone

    # 1. EXIF datetime + offset — most accurate for native camera photos
    if item.exif:
        offset = item.exif.get("OffsetTimeOriginal") or item.exif.get("OffsetTime")
        for key in ("DateTimeOriginal", "DateTimeDigitized"):
            val = item.exif.get(key)
            if val:
                try:
                    if offset:
                        dt = datetime.strptime(f"{val}{offset}", "%Y:%m:%d %H:%M:%S%z")
                    else:
                        dt = datetime.strptime(val, "%Y:%m:%d %H:%M:%S")
                    return dt.astimezone(dt_timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                except ValueError:
                    continue

    # 2. date_created / taken_at from Shortcuts — carries tz offset, used for Snapchat etc.
    for ts in (item.date_created, item.taken_at):
        if ts:
            try:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                return dt.astimezone(dt_timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            except ValueError:
                continue

    return None


def _resolve_source_app(item: PhotoItem) -> str | None:
    """Determine source app from album name, camera make, or UserComment."""
    # Album name is most reliable
    if item.album:
        for app in ("snapchat", "instagram", "whatsapp", "vsco", "lightroom"):
            if app in item.album.lower():
                return app

    # Empty camera make → saved by a third-party app
    # Try to identify via UserComment
    if not item.camera_make:
        if item.exif:
            raw = item.exif.get("UserComment")
            if raw:
                try:
                    decoded = b64decode(raw).decode("utf-8", errors="ignore").lower()
                    for app in ("snapchat", "instagram", "whatsapp", "vsco"):
                        if app in decoded:
                            return app
                except Exception as exc:
                    logger.debug("Failed to decode EXIF UserComment for %s: %s", item.filename, exc)
        return "unknown"

    return "camera"


def _normalise_media_type(item: PhotoItem) -> str:
    if item.is_screen_recording:
        return "screen_recording"
    if item.is_screenshot:
        return "screenshot"
    if item.duration_s:
        return "video"
    return "photo"


def _insert_batch(items: list[PhotoItem]) -> None:
    inserted = 0
    skipped = 0
    no_timestamp = 0

    for item in items:
        if not item.file_path:
            logger.warning("No file_path for %s — skipping", item.filename)
            skipped += 1
            continue

        taken_at = _parse_taken_at(item)
        if not taken_at:
            logger.warning("No resolvable timestamp for %s — skipping", item.file_path)
            no_timestamp += 1
            continue

        # Guard against Shortcuts sending 0.0 instead of null for missing GPS
        lat = item.location_lat if item.location_lat else None
        lon = item.location_lon if item.location_lon else None
        gps_source = "exif" if (lat is not None and lon is not None) else None
        place_id = get_place_id(lat, lon) if gps_source else None

        record = PhotoMetadataRecord(
            file_path=item.file_path,
            taken_at=taken_at,
            is_screenshot=int(item.is_screenshot),
            is_screen_recording=int(item.is_screen_recording),
            is_favourite=int(item.is_favourite),
            filename=item.filename,
            file_extension=item.file_extension,
            file_size_bytes=item.file_size_bytes,
            date_created=item.date_created,
            latitude=lat,
            longitude=lon,
            gps_source=gps_source,
            place_id=place_id,
            media_type=_normalise_media_type(item),
            photo_type=item.photo_type or None,
            width=item.width,
            height=item.height,
            duration_s=item.duration_s,
            source_app=_resolve_source_app(item),
            camera_make=item.camera_make or None,
            camera_model=item.camera_model or None,
            album=item.album or None,
            raw_exif=item.exif,  # table.insert() handles dict → JSON serialisation
        )

        was_inserted = photos_table.insert(record)
        if was_inserted:
            inserted += 1
        else:
            skipped += 1

    logger.info(
        "Photo metadata batch complete — inserted=%d, skipped=%d, no_timestamp=%d",
        inserted, skipped, no_timestamp,
    )


@router.post("", dependencies=[Depends(require_upload_token)])
async def upload_photos(
    request: Request,
    background_tasks: BackgroundTasks,
):
    body = await request.json()
    photos_raw = json.loads(body["photos"])

    items = [PhotoItem(**p) for p in photos_raw]
    background_tasks.add_task(_insert_batch, items)
    return {"status": "success", "queued": len(items)}