from datetime import datetime, timezone
import hashlib
import json
import logging
from typing import Optional
from fastapi import APIRouter, UploadFile, File, Header, HTTPException, BackgroundTasks #type: ignore
from pydantic import BaseModel, Field, field_validator #type: ignore

from auth import check_auth 
from database.exchange.util import convert_to_gbp
from database.util import get_conn
from upload.transaction.util import parse_revolut_upload, parse_wise_upload

router = APIRouter()
logger = logging.getLogger(__name__)

@router.post("/wise")
async def upload_wise(background_tasks: BackgroundTasks,
                      file: UploadFile = File(...),
                      authorization: str = Header(...)):
    check_auth(authorization)

    if not file.filename.endswith(".zip"):
        raise HTTPException(status_code=400, detail="File must be a .zip")

    background_tasks.add_task(parse_wise_upload, file)
    
    return {"status": "ok"}

@router.post("/revolut")
async def upload_revolut(background_tasks: BackgroundTasks,
                         file: UploadFile = File(...),
                         authorization: str = Header(None)):
    check_auth(authorization)

    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="File must be a CSV")

    background_tasks.add_task(parse_revolut_upload, file)

    return {"status": "ok"}

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