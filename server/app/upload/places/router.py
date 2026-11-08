import logging
from typing import Optional

from auth import require_upload_token
from config import general
from database.connection import get_conn
from database.location.geocoding import insert_geocode, reverse_geocode
from pydantic import BaseModel #type: ignore
from fastapi import APIRouter, Depends, HTTPException  # type: ignore
from database.location.known_places.table import table as known_places_table
from database.connection import to_iso_str
from datetime import datetime, timezone
from database.location.sub_places.table import (
    table as sub_places_table,
    SubPlaceRecord,
    DEFAULT_RADIUS_M,
)
from triggers.location_change import (
    add_known_place_now,
    AlreadyKnownPlaceError,
    NoRecentLocationError,
)

router = APIRouter()
logger = logging.getLogger(__name__)

class PlaceRequest(BaseModel):
    place_id: int
    label: str
    notes: str

class PlaceUpdateRequest(BaseModel):
    label: Optional[str] = None
    notes: Optional[str] = None

class DeletePlaceRequest(BaseModel):
    confirm_id: int   # must equal the place id in the URL

class AddCurrentPlaceRequest(BaseModel):
    label: Optional[str] = None
    notes: Optional[str] = None

class SubPlaceCreateRequest(BaseModel):
    name: str
    type: Optional[str] = None          # free entry; created in sub_place_types if new
    notes: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    radius_m: int = DEFAULT_RADIUS_M
    wifi_ssid: Optional[str] = None

class SubPlaceUpdateRequest(BaseModel):
    name: Optional[str] = None
    type: Optional[str] = None
    notes: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    radius_m: Optional[int] = None
    wifi_ssid: Optional[str] = None

class VisitUpdateRequest(BaseModel):
    notes: Optional[str] = None


def _place_row_to_dict(row) -> dict:
    d = dict(row)
    return {
        "id": d["id"],
        "label": d["label"],
        "notes": d["notes"],
        "latitude": d["latitude"],
        "longitude": d["longitude"],
        "display_name": d.get("display_name"),
        "timezone": d.get("timezone"),
        "first_seen": d["first_seen"],
        "last_visited": d.get("last_visited"),
        "visit_count": d["visit_count"],
        "total_time_mins": d["total_time_mins"],
        "is_geocoded": bool(d.get("geocoded_at")),
    }


def _visit_row_to_dict(row) -> dict:
    d = dict(row)
    return {
        "id": d["id"],
        "arrived_at": d["arrived_at"],
        "departed_at": d.get("departed_at"),
        "duration_mins": d.get("duration_mins"),
        "notes": d.get("notes"),
        "is_ongoing": d.get("departed_at") is None,
    }

@router.get("/list-null", dependencies=[Depends(require_upload_token)])
async def list_null_places():
    """List all places with null names"""
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT
                kp.id, kp.latitude, kp.longitude, kp.first_seen,
                p.country, p.city, p.suburb, p.road, p.display_name, p.timezone,
                p.lat_snap, p.lon_snap
            FROM known_places kp
            LEFT JOIN places p
                ON p.lat_snap = ROUND(kp.latitude, 3)
                AND p.lon_snap = ROUND(kp.longitude, 3)
            WHERE kp.label IS NULL AND kp.deleted_at IS NULL
            ORDER BY kp.first_seen DESC
        """).fetchall()
    return {"places": [dict(row) for row in rows]}

@router.post("/update-label", dependencies=[Depends(require_upload_token)])
async def update_place_label(request: PlaceRequest):
    """Update the label for a given place_id"""
    notes = None if request.notes == '' else request.notes
    with get_conn() as conn:
        result = conn.execute("""
            UPDATE known_places
            SET label = ?, notes = ?
            WHERE id = ?
        """, (request.label, notes, request.place_id))
        if result.rowcount == 0:
            return {"status": "error", "message": "Place ID not found"}
    return {"status": "success"}


@router.get("/list", dependencies=[Depends(require_upload_token)])
async def list_places():
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT
                kp.id, kp.label, kp.notes, kp.latitude, kp.longitude,
                kp.first_seen, kp.last_visited, kp.visit_count, kp.total_time_mins,
                p.display_name, p.geocoded_at, p.timezone
            FROM known_places kp
            LEFT JOIN places p ON p.id = kp.place_id
            WHERE kp.deleted_at IS NULL
            ORDER BY kp.last_visited DESC, kp.first_seen DESC
        """).fetchall()
    return {"places": [_place_row_to_dict(r) for r in rows]}


@router.post("/add-current", dependencies=[Depends(require_upload_token)])
async def add_current_place(request: AddCurrentPlaceRequest):
    """Instantly add the current location as a known place, skipping the stay-duration threshold."""
    try:
        place_id = add_known_place_now(label=request.label, notes=request.notes)
    except AlreadyKnownPlaceError as e:
        raise HTTPException(status_code=409, detail={
            "message": "Current location is already within a known place",
            "place_id": e.place_id,
            "label": e.label,
        })
    except NoRecentLocationError as e:
        raise HTTPException(status_code=422, detail={"message": str(e)})
    with get_conn() as conn:
        row = conn.execute("""
            SELECT
                kp.id, kp.label, kp.notes, kp.latitude, kp.longitude,
                kp.first_seen, kp.last_visited, kp.visit_count, kp.total_time_mins,
                p.display_name, p.geocoded_at, p.timezone
            FROM known_places kp
            LEFT JOIN places p ON p.id = kp.place_id
            WHERE kp.id = ?
        """, (place_id,)).fetchone()
    return _place_row_to_dict(row)


@router.patch("/visits/{visit_id}", dependencies=[Depends(require_upload_token)])
async def update_visit(visit_id: int, request: VisitUpdateRequest):
    with get_conn() as conn:
        row = conn.execute("SELECT id FROM place_visits WHERE id = ?", (visit_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Visit not found")
        if "notes" in request.__fields_set__:
            conn.execute("UPDATE place_visits SET notes = ? WHERE id = ?", (request.notes, visit_id))
        updated = conn.execute(
            "SELECT id, arrived_at, departed_at, duration_mins, notes FROM place_visits WHERE id = ?",
            (visit_id,)
        ).fetchone()
    return _visit_row_to_dict(updated)


@router.patch("/{place_id}", dependencies=[Depends(require_upload_token)])
async def update_place(place_id: int, request: PlaceUpdateRequest):
    with get_conn() as conn:
        row = conn.execute("SELECT id FROM known_places WHERE id = ? AND deleted_at IS NULL", (place_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Place not found")
        fields = request.__fields_set__
        if fields:
            set_parts = []
            values = []
            if "label" in fields:
                set_parts.append("label = ?")
                values.append(request.label)
            if "notes" in fields:
                set_parts.append("notes = ?")
                values.append(request.notes)
            if set_parts:
                conn.execute(
                    f"UPDATE known_places SET {', '.join(set_parts)} WHERE id = ?",
                    (*values, place_id),
                )
        updated = conn.execute("""
            SELECT
                kp.id, kp.label, kp.notes, kp.latitude, kp.longitude,
                kp.first_seen, kp.last_visited, kp.visit_count, kp.total_time_mins,
                p.display_name, p.geocoded_at, p.timezone
            FROM known_places kp
            LEFT JOIN places p ON p.id = kp.place_id
            WHERE kp.id = ?
        """, (place_id,)).fetchone()
    return _place_row_to_dict(updated)


@router.post("/{place_id}/geocode", dependencies=[Depends(require_upload_token)])
def geocode_place(place_id: int):
    """Trigger on-demand Nominatim geocode for a known place. Runs in thread pool (blocking)."""
    with get_conn() as conn:
        kp = conn.execute("SELECT * FROM known_places WHERE id = ? AND deleted_at IS NULL", (place_id,)).fetchone()
        if not kp:
            raise HTTPException(status_code=404, detail="Place not found")
        kp = dict(kp)
        pid = kp["place_id"]
        if pid is None:
            lat_snap = round(kp["latitude"], 3)
            lon_snap = round(kp["longitude"], 3)
            conn.execute(
                "INSERT OR IGNORE INTO places (lat_snap, lon_snap) VALUES (?, ?)",
                (lat_snap, lon_snap),
            )
            row = conn.execute(
                "SELECT id FROM places WHERE lat_snap = ? AND lon_snap = ?",
                (lat_snap, lon_snap),
            ).fetchone()
            pid = row[0]
            conn.execute(
                "UPDATE known_places SET place_id = ? WHERE id = ?",
                (pid, place_id),
            )

    geocode = reverse_geocode(kp["latitude"], kp["longitude"])
    if geocode is None:
        raise HTTPException(status_code=503, detail="Nominatim geocoding failed or is unreachable")

    insert_geocode(pid, geocode)
    return {"display_name": geocode.get("display_name")}


@router.get("/{place_id}/visits", dependencies=[Depends(require_upload_token)])
async def list_place_visits(place_id: int):
    with get_conn() as conn:
        kp = conn.execute("""
            SELECT kp.id, p.timezone
            FROM known_places kp
            LEFT JOIN places p ON p.id = kp.place_id
            WHERE kp.id = ? AND kp.deleted_at IS NULL
        """, (place_id,)).fetchone()
        if not kp:
            raise HTTPException(status_code=404, detail="Place not found")
        rows = conn.execute("""
            SELECT id, arrived_at, departed_at, duration_mins, notes
            FROM place_visits_cleaned
            WHERE known_place_id = ?
            ORDER BY arrived_at DESC
        """, (place_id,)).fetchall()
    return {
        "timezone": dict(kp).get("timezone"),
        "visits": [_visit_row_to_dict(r) for r in rows],
    }

# ---------------------------------------------------------------------------
# Delete / restore (soft)
# ---------------------------------------------------------------------------

@router.delete("/{place_id}", dependencies=[Depends(require_upload_token)])
async def delete_place(place_id: int, request: DeletePlaceRequest):
    """Soft-delete a known place. The body must repeat the id (``confirm_id``) so a
    stray or blind DELETE cannot remove a place. Rows are kept and can be restored."""
    if request.confirm_id != place_id:
        raise HTTPException(status_code=400, detail="confirm_id does not match place id")
    with get_conn(read_only=True) as conn:
        row = conn.execute("""
            SELECT kp.id, kp.label, kp.visit_count, kp.total_time_mins,
                   (SELECT COUNT(*) FROM sub_places sp
                     WHERE sp.known_place_id = kp.id AND sp.archived_at IS NULL) AS sub_places
            FROM known_places kp WHERE kp.id = ? AND kp.deleted_at IS NULL
        """, (place_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Place not found")
    if not known_places_table.soft_delete_place(place_id, to_iso_str(datetime.now(timezone.utc))):
        raise HTTPException(status_code=404, detail="Place not found")
    logger.warning("Known place %d (%r) deleted via API: %d visit(s), %d sub-place(s) hidden",
                   place_id, row["label"], row["visit_count"], row["sub_places"])
    return {
        "status": "deleted", "id": place_id, "label": row["label"],
        "visits_hidden": row["visit_count"], "sub_places_hidden": row["sub_places"],
    }


@router.post("/{place_id}/restore", dependencies=[Depends(require_upload_token)])
async def restore_place(place_id: int):
    if not known_places_table.restore_place(place_id):
        raise HTTPException(status_code=404, detail="No deleted place with that id")
    return {"status": "restored", "id": place_id}


# ---------------------------------------------------------------------------
# Sub-places
# ---------------------------------------------------------------------------

def _clean(value: Optional[str]) -> Optional[str]:
    """Strip a string; empty becomes None."""
    if value is None:
        return None
    value = value.strip()
    return value or None


def _validate_geo(lat, lon, radius) -> None:
    if (lat is None) != (lon is None):
        raise HTTPException(status_code=422, detail="latitude and longitude must be set together")
    if lat is not None and not (-90 <= lat <= 90 and -180 <= lon <= 180):
        raise HTTPException(status_code=422, detail="latitude/longitude out of range")
    if radius is not None and not (5 <= radius <= 500):
        raise HTTPException(status_code=422, detail="radius_m must be between 5 and 500")


def _require_place(place_id: int) -> None:
    with get_conn(read_only=True) as conn:
        if not conn.execute("SELECT 1 FROM known_places WHERE id = ? AND deleted_at IS NULL", (place_id,)).fetchone():
            raise HTTPException(status_code=404, detail="Place not found")


@router.get("/sub-place-types", dependencies=[Depends(require_upload_token)])
async def list_sub_place_types():
    return {"types": sub_places_table.list_types()}


@router.get("/{place_id}/sub-places", dependencies=[Depends(require_upload_token)])
async def list_sub_places(place_id: int):
    _require_place(place_id)
    return {
        "sub_places": sub_places_table.list_for_place(place_id),
        # Source of truth for the UI's "pin is outside the parent place" warning.
        "parent_radius_m": general.LOCATION_CHANGE_RADIUS_M,
    }


@router.post("/{place_id}/sub-places", dependencies=[Depends(require_upload_token)])
async def create_sub_place(place_id: int, request: SubPlaceCreateRequest):
    _require_place(place_id)
    name = _clean(request.name)
    if not name:
        raise HTTPException(status_code=422, detail="name is required")
    _validate_geo(request.latitude, request.longitude, request.radius_m)
    type_name = _clean(request.type)
    sub_id = sub_places_table.insert(SubPlaceRecord(
        known_place_id=place_id,
        name=name,
        type_id=sub_places_table.get_or_create_type(type_name) if type_name else None,
        notes=_clean(request.notes),
        latitude=request.latitude,
        longitude=request.longitude,
        radius_m=request.radius_m,
        wifi_ssid=_clean(request.wifi_ssid),
    ))
    return sub_places_table.get(sub_id)


@router.patch("/sub-places/{sub_place_id}", dependencies=[Depends(require_upload_token)])
async def update_sub_place(sub_place_id: int, request: SubPlaceUpdateRequest):
    current = sub_places_table.get(sub_place_id)
    if not current or current["archived_at"]:
        raise HTTPException(status_code=404, detail="Sub-place not found")
    sent = request.model_fields_set
    fields = {}
    if "name" in sent:
        name = _clean(request.name)
        if not name:
            raise HTTPException(status_code=422, detail="name cannot be empty")
        fields["name"] = name
    if "type" in sent:
        type_name = _clean(request.type)
        fields["type_id"] = sub_places_table.get_or_create_type(type_name) if type_name else None
    if "notes" in sent:
        fields["notes"] = _clean(request.notes)
    if "wifi_ssid" in sent:
        fields["wifi_ssid"] = _clean(request.wifi_ssid)
    if "radius_m" in sent:
        if request.radius_m is None:
            raise HTTPException(status_code=422, detail="radius_m cannot be null")
        fields["radius_m"] = request.radius_m
    if "latitude" in sent or "longitude" in sent:
        fields["latitude"] = request.latitude if "latitude" in sent else current["latitude"]
        fields["longitude"] = request.longitude if "longitude" in sent else current["longitude"]
    _validate_geo(fields.get("latitude", current["latitude"]),
                  fields.get("longitude", current["longitude"]),
                  fields.get("radius_m"))
    sub_places_table.update(sub_place_id, **fields)
    return sub_places_table.get(sub_place_id)


@router.delete("/sub-places/{sub_place_id}", dependencies=[Depends(require_upload_token)])
async def archive_sub_place(sub_place_id: int):
    """Soft-delete: history is retained, the sub-place is hidden from listings."""
    if not sub_places_table.archive_sub_place(sub_place_id):
        raise HTTPException(status_code=404, detail="Sub-place not found")
    return {"status": "archived"}
