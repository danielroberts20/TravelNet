from datetime import datetime, timezone
import hashlib
import io
import json
import logging
import zipfile
from babel import numbers
from typing import Optional
from notifications import send_notification
from config.general import REVOLUT_BACKUP_DIR, WISE_BACKUP_DIR
from fastapi import APIRouter, UploadFile, File, HTTPException, BackgroundTasks, Depends  # type: ignore
from pydantic import BaseModel, Field, field_validator  # type: ignore
from database.transaction.ingest.revolut import insert as insert_revolut
from database.transaction.ingest.wise import insert as insert_wise

from auth import require_upload_token
from database.exchange.util import convert_to_gbp
from database.util import get_conn
from upload.transaction.constants import WISE_SOURCE_MAP

router = APIRouter()
logger = logging.getLogger(__name__)


def _process_wise(contents: bytes) -> None:
    """Parse and ingest all CSVs from a Wise zip export (runs in background)."""
    try:
        zf = zipfile.ZipFile(io.BytesIO(contents))
    except zipfile.BadZipFile:
        logger.error("Wise background task: invalid or corrupted zip")
        return

    for filename in [f for f in zf.namelist() if f.endswith(".csv")]:
        try:
            parts = filename.split("_")
            source = "_".join([parts[1], parts[2]])
            insert_wise(zf, filename, source)
        except Exception as e:
            logger.error("Wise background task: error processing %s: %s", filename, e)


@router.post("/wise", dependencies=[Depends(require_upload_token)])
async def upload_wise(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    """Accept a Wise .zip export, validate it, save a backup, and queue ingestion."""
    if not file.filename.endswith(".zip"):
        raise HTTPException(status_code=400, detail="File must be a .zip")

    contents = await file.read()

    try:
        zf = zipfile.ZipFile(io.BytesIO(contents))
    except zipfile.BadZipFile:
        raise HTTPException(status_code=400, detail="Invalid or corrupted zip file")

    csv_files = [f for f in zf.namelist() if f.endswith(".csv")]
    if not csv_files:
        raise HTTPException(status_code=400, detail="No CSV files found in zip")

    now = datetime.now()
    backup_path = WISE_BACKUP_DIR / f"{now.strftime('%Y-%m-%d_%H-%M-%S')}.zip"
    with open(backup_path, "wb") as f:
        f.write(contents)

    background_tasks.add_task(_process_wise, contents)
    return {"status": "queued", "files": csv_files}


@router.post("/revolut", dependencies=[Depends(require_upload_token)])
async def upload_revolut(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    """Accept a Revolut CSV export, save a backup, and queue ingestion."""
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="File must be a CSV")

    contents = await file.read()
    decoded = contents.decode("utf-8")

    now = datetime.now()
    backup_path = REVOLUT_BACKUP_DIR / f"{now.strftime('%Y-%m-%d_%H-%M-%S')}.csv"
    with open(backup_path, "w+") as f:
        f.write(decoded)

    background_tasks.add_task(insert_revolut, decoded)
    return {"status": "queued"}


class CashTransactionRequest(BaseModel):
    """Request body for manually logging a cash transaction."""

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
    def uppercase_currency(cls, v: str) -> str:
        """Normalise currency code to upper-case."""
        return v.upper()

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp(cls, v: Optional[str]) -> Optional[str]:
        """Reject timestamps that are not valid ISO8601 strings."""
        if v is None:
            return v
        try:
            datetime.fromisoformat(v)
        except ValueError:
            raise ValueError("timestamp must be a valid ISO8601 datetime string e.g. '2026-09-14T14:30:00'")
        return v


class CashTransactionResponse(BaseModel):
    """Response body returned after successfully logging a cash transaction."""

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

    currency_symbol = numbers.get_currency_symbol(tx.currency, locale="en_GB")
    send_notification(
        title="Cash 💸",
        body=f"{"-" if tx.amount < 0 else "+"}{currency_symbol}{abs(tx.amount):.2f} ({tx.currency}) logged",
        time_sensitive=False
    )
    return CashTransactionResponse(
        id=tx_id,
        timestamp=timestamp,
        amount=tx.amount,
        currency=tx.currency,
        amount_gbp=amount_gbp,
        description=tx.description,
        message="Cash transaction recorded successfully.",
    )
