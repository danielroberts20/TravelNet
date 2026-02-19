import csv
from datetime import datetime, timedelta, timezone
import io
import os
import time
from pathlib import Path
from fastapi import FastAPI, HTTPException, UploadFile, File, Header, Body, Query # type: ignore
from database import get_conn, insert_fx_rate, insert_log
from telemetry_models import Log
from logging_config import configure_logging
import logging

configure_logging()
logger = logging.getLogger(__name__)

# Storage directory (Docker volume)
UPLOAD_DIR = Path("/data")
UPLOAD_DIR.mkdir(exist_ok=True)

# Optional auth token
UPLOAD_TOKEN = os.getenv("UPLOAD_TOKEN", None)

app = FastAPI(title="Pi Upload Service")


def check_auth(authorization: str):
    if UPLOAD_TOKEN:
        if not authorization or authorization != f"Bearer {UPLOAD_TOKEN}":
            raise HTTPException(status_code=401, detail="Unauthorized")


@app.post("/upload_json")
async def upload_json(
    data: dict = Body(...),
    authorization: str = Header(None)
):
    check_auth(authorization)

    filename = f"{int(time.time())}.json"
    filepath = UPLOAD_DIR / filename

    with filepath.open("w", encoding="utf-8") as f:
        import json
        json.dump(data, f, indent=2)

    return {"status": "saved", "filename": filename}


@app.post("/upload_text")
async def upload_text(
    text: str = Body(..., media_type="text/plain"),
    authorization: str = Header(None)
):
    check_auth(authorization)

    filename = f"{int(time.time())}.txt"
    filepath = UPLOAD_DIR / filename

    with filepath.open("w", encoding="utf-8") as f:
        f.write(text)

    return {"status": "saved", "filename": filename}

@app.post("/upload_fx")
async def upload_json(
    data: dict = Body(...),
    authorization: str = Header(None)
):
    check_auth(authorization)
    output = []

    for item in data["quotes"]:
        target = item[len(data["source"]):]
        output.append(insert_fx_rate(data["date"], data["source"], target, data["quotes"][item], data["timestamp"]))
    return output

@app.post("/upload_loc")
async def upload_json(
    data: dict = Body(...),
    authorization: str = Header(None)
):
    check_auth(authorization)

    return {}

@app.get("/locations/recent")
def get_recent_locations(days: int = Query(7, ge=1, le=365)):
    cutoff = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())

    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT lat, lon, timestamp, formatted, source
            FROM locations
            WHERE timestamp >= ?
            ORDER BY timestamp DESC
            """,
            (cutoff,)
        ).fetchall()

    return [dict(row) for row in rows]

@app.post("/upload_csv")
async def upload_csv(file: UploadFile = File(...),
                     authorization: str = Header(None)):

    check_auth(authorization)

    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="File must be a CSV")

    contents = await file.read()

    # Decode bytes → string
    decoded = contents.decode("utf-8")

    # Convert string → file-like object
    csv_file = io.StringIO(decoded)

    reader = csv.DictReader(csv_file)

    required_fields = {"latitude", "longitude", "timestamp"}

    if not required_fields.issubset(reader.fieldnames):
        raise HTTPException(
            status_code=400,
            detail=f"CSV must contain columns: {required_fields}"
        )

    inserted = 0
    skipped_rows = []


    for idx, row in enumerate(reader):
        try:
            log = Log.from_strings(**row)
            insert_log(log)
            inserted += 1
        except Exception as e:
            # Skip bad rows
            logger.warning(f"Bad row on line {idx+2}.\t CSV entry: {row}\tException: {e}")
            skipped_rows.append(idx+2)
            continue
    
    logger.info(f"Successfully uploaded {inserted}/{len(row)} entries")
    return {
        "status": "success",
        "rows_inserted": inserted,
        "skipped_rows": skipped_rows
    }