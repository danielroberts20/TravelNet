from fastapi import HTTPException, Header, status  # type: ignore

from config.settings import settings

def check_auth(authorization: str):
    if settings.upload_token:
        if not authorization or authorization != f"Bearer {settings.upload_token}":
            raise HTTPException(status_code=401, detail="Unauthorized")

def verify_token(authorization: str = Header(...)):
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or token != settings.overland_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing bearer token",
        )