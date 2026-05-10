import logging
from typing import Optional

from auth import require_upload_token
from database.connection import get_conn
from database.location.geocoding import insert_geocode, reverse_geocode
from pydantic import BaseModel #type: ignore
from fastapi import APIRouter, Depends, HTTPException  # type: ignore

router = APIRouter()
logger = logging.getLogger(__name__)

class PlaceRequest(BaseModel):
    place_id: int
    label: str
    notes: str

class PlaceUpdateRequest(BaseModel):
    label: Optional[str] = None
    notes: Optional[str] = None

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
            WHERE kp.label IS NULL
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
                p.display_name, p.geocoded_at
            FROM known_places kp
            LEFT JOIN places p ON p.id = kp.place_id
            ORDER BY kp.last_visited DESC, kp.first_seen DESC
        """).fetchall()
    return {"places": [_place_row_to_dict(r) for r in rows]}


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
        row = conn.execute("SELECT id FROM known_places WHERE id = ?", (place_id,)).fetchone()
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
                p.display_name, p.geocoded_at
            FROM known_places kp
            LEFT JOIN places p ON p.id = kp.place_id
            WHERE kp.id = ?
        """, (place_id,)).fetchone()
    return _place_row_to_dict(updated)


@router.post("/{place_id}/geocode", dependencies=[Depends(require_upload_token)])
def geocode_place(place_id: int):
    """Trigger on-demand Nominatim geocode for a known place. Runs in thread pool (blocking)."""
    with get_conn() as conn:
        kp = conn.execute("SELECT * FROM known_places WHERE id = ?", (place_id,)).fetchone()
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
        kp = conn.execute("SELECT id FROM known_places WHERE id = ?", (place_id,)).fetchone()
        if not kp:
            raise HTTPException(status_code=404, detail="Place not found")
        rows = conn.execute("""
            SELECT id, arrived_at, departed_at, duration_mins, notes
            FROM place_visits
            WHERE known_place_id = ?
            ORDER BY arrived_at DESC
        """, (place_id,)).fetchall()
    return {"visits": [_visit_row_to_dict(r) for r in rows]}