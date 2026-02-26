from fastapi import HTTPException  # type: ignore

from config.general import UPLOAD_TOKEN


def check_auth(authorization: str):
    if UPLOAD_TOKEN:
        if not authorization or authorization != f"Bearer {UPLOAD_TOKEN}":
            raise HTTPException(status_code=401, detail="Unauthorized")