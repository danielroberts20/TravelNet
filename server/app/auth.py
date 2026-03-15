from fastapi import HTTPException, Header, status  # type: ignore

from config.general import UPLOAD_TOKEN, OVERLAND_TOKEN

def check_auth(authorization: str):
    if UPLOAD_TOKEN:
        if not authorization or authorization != f"Bearer {UPLOAD_TOKEN}":
            raise HTTPException(status_code=401, detail="Unauthorized")

def verify_token(authorization: str = Header(...)):
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or token != OVERLAND_TOKEN:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing bearer token",
        )