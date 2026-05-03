import logging

from auth import require_upload_token
from database.connection import get_conn
from pydantic import BaseModel #type: ignore
from fastapi import APIRouter, Depends  # type: ignore

router = APIRouter()
logger = logging.getLogger(__name__)

class PlaceRequest(BaseModel):
    place_id: int
    label: str
    notes: str

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