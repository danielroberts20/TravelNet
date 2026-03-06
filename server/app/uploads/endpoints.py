import csv
from datetime import datetime, timezone
import hashlib
import io
import json
from fastapi import APIRouter, Header, BackgroundTasks, UploadFile, File, HTTPException, Depends, status  #type: ignore
import logging
from typing import Any, Optional
from pydantic import BaseModel, Field, field_validator

from auth import check_auth, verify_token
from config.general import HEALTH_BACKUP_DIR, LOCATION_BACKUP_DIR, REVOLUT_TRANSACTION_BACKUP_DIR, WISE_TRANSACTION_BACKUP_DIR
from database.transaction.ingest.wise import insert as insert_wise
from database.transaction.ingest.revolut import insert as insert_revolut
from database.exchange.util import convert_to_gbp
from database.util import get_conn
from database.location.table import insert_location
from telemetry_models import OverlandPayload
from uploads.utils import handle_health_upload, input_csv

router = APIRouter()
logger = logging.getLogger(__name__)

@router.post("/health")
async def upload_health(data: dict[str, Any],
                        background_tasks: BackgroundTasks,
                        authorization: str = Header(...)):

    check_auth(authorization)

    now = datetime.now()
    year_month = now.strftime("%Y-%m")
    day = int(now.strftime("%d"))-1
    with open(HEALTH_BACKUP_DIR / f"{year_month}-{day}.json", "w+") as f:
        f.write(str(data))
        f.close()

    background_tasks.add_task(handle_health_upload, data)    
    logger.info(f"Successfully uploaded {len(data)} health entries")
    return {
        "status": "success",
        "csvs_received": len(data)
    }

@router.post("/csv")
async def upload_csv(file: UploadFile = File(...),
                     authorization: str = Header(None)):

    check_auth(authorization)

    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="File must be a CSV")

    contents = await file.read()

    # Decode bytes → string
    decoded = contents.decode("utf-8")
    now = datetime.now()
    year_month = now.strftime("%Y-%m")
    day = int(now.strftime("%d"))-1
    with open(LOCATION_BACKUP_DIR / f"{year_month}-{day}.csv", "w+") as f:
        f.write(decoded)
        f.close()

    # Convert string → file-like object
    csv_file = io.StringIO(decoded)
    
    inserted, skipped_rows = input_csv(csv_file)

    return {
        "status": "success",
        "rows_inserted": inserted,
        "skipped_rows": skipped_rows
    }

@router.post("/wise")
async def upload_wise(file: UploadFile = File(...),
                      authorization: str = Header(None),
                      source: str = Header(None)):
    check_auth(authorization)

    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="File must be a CSV")

    print(source)
    
    contents = await file.read()

    # Decode bytes → string
    decoded = contents.decode("utf-8")
    now = datetime.now()
    year_month = now.strftime("%Y-%m")
    day = int(now.strftime("%d"))-1
    csv_path = WISE_TRANSACTION_BACKUP_DIR / f"{year_month}-{day}.csv"
    with open(csv_path, "w+") as f:
        f.write(decoded)
        f.close()

    inserted, skipped, errors = insert_wise(csv_path, source)

    return {
        "inserted": inserted,
        "skipped": skipped,
        "errors": errors
    }

@router.post("/revolut")
async def upload_revolut(file: UploadFile = File(...),
                      authorization: str = Header(None)):
    check_auth(authorization)

    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="File must be a CSV")
    
    contents = await file.read()

    # Decode bytes → string
    decoded = contents.decode("utf-8")
    now = datetime.now()
    year_month = now.strftime("%Y-%m")
    day = int(now.strftime("%d"))-1
    csv_path = REVOLUT_TRANSACTION_BACKUP_DIR / f"{year_month}-{day}.csv"
    with open(csv_path, "w+") as f:
        f.write(decoded)
        f.close()

    inserted, skipped, errors = insert_revolut(csv_path)

    return {
        "inserted": inserted,
        "skipped": skipped,
        "errors": errors
    }

class CashTransactionRequest(BaseModel):
    amount: float = Field(..., description="Transaction amount. Negative for spend, positive for received.")
    currency: str = Field(..., min_length=3, max_length=3, description="ISO 4217 currency code e.g. AUD, USD, THB")
    description: str = Field(..., min_length=1, max_length=500, description="What was this for?")
    timestamp: Optional[str] = Field(
        None,
        description="ISO8601 datetime. Defaults to now if omitted. e.g. '2026-09-14T14:30:00'"
    )
    note: Optional[str] = Field(None, description="Any extra notes")

    @field_validator("currency")
    @classmethod
    def uppercase_currency(cls, v):
        return v.upper()

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp(cls, v):
        if v is None:
            return v
        try:
            datetime.fromisoformat(v)
        except ValueError:
            raise ValueError("timestamp must be a valid ISO8601 datetime string e.g. '2026-09-14T14:30:00'")
        return v


class CashTransactionResponse(BaseModel):
    id: str
    timestamp: str
    amount: float
    currency: str
    amount_gbp: Optional[float]
    description: str
    message: str


@router.post("/cash", response_model=CashTransactionResponse)
async def add_cash_transaction(tx: CashTransactionRequest):
    """
    Manually log a cash transaction in any currency.
    Used for cash spend/received that won't appear in any bank export.
    """
    # Resolve timestamp
    if tx.timestamp:
        timestamp = datetime.fromisoformat(tx.timestamp).isoformat()
    else:
        timestamp = datetime.now(timezone.utc).isoformat()

    tx_date = datetime.fromisoformat(timestamp).date()

    # Generate stable ID
    key = f"cash-{timestamp}-{tx.amount}-{tx.currency}-{tx.description}"
    tx_id = "CASH-" + hashlib.sha256(key.encode()).hexdigest()[:16]

    # FX conversion
    amount_gbp = convert_to_gbp(tx.amount, tx.currency, tx_date)

    raw_json = json.dumps({
        "amount": tx.amount,
        "currency": tx.currency,
        "description": tx.description,
        "timestamp": timestamp,
        "note": tx.note,
    })

    with get_conn() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                INSERT OR IGNORE INTO transactions (
                    id, source, bank, timestamp, amount, currency, amount_gbp,
                    description, payment_reference, payer, payee, merchant,
                    fees, transaction_type, transaction_detail, state,
                    is_internal, is_interest, running_balance, raw
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    tx_id, "cash", "Cash", timestamp, tx.amount, tx.currency, amount_gbp,
                    tx.description, None, None, None, None,
                    0.0, "DEBIT" if tx.amount < 0 else "CREDIT", "CASH", "COMPLETED",
                    0, 0, None, raw_json,
                ),
            )
            # if cursor.rowcount == 0:
            #     raise HTTPException(status_code=409, detail=f"Duplicate transaction ID: {tx_id}")
            logger.info(f"Cash insert rowcount: {cursor.rowcount}")
            conn.commit()
        except Exception as e:
            logger.error(f"Cash insert error: {e}")
            raise HTTPException(status_code=500, detail=f"DB error: {e}")

    return CashTransactionResponse(
        id=tx_id,
        timestamp=timestamp,
        amount=tx.amount,
        currency=tx.currency,
        amount_gbp=amount_gbp,
        description=tx.description,
        message="Cash transaction recorded successfully.",
    )

@router.post(
    "/overland",
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(verify_token)],
)
async def upload_overland(
        payload: OverlandPayload,
):
    for item in payload.locations:
        insert_location(
            conn=get_conn(),
            timestamp=item.properties.timestamp,
            timezone="",
            latitude=item.geometry.coordinates[1],
            longitude=item.geometry.coordinates[0],
            altitude=item.properties.altitude,
            activity=str(item.properties.motion),
            device="overland",
            is_locked=None,
            battery=int(item.properties.battery_level * 100),
            is_charging=None,
            is_connected_charger=None,
            BSSID=None,
            RSSI=None
        )
    return {"result": "ok"}

@router.post(
    "/discard",
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(verify_token)],
)
async def discard_overland():
    return {"result": "ok"}