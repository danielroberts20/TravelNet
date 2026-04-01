"""
auth.py
~~~~~~~
FastAPI dependency functions for bearer-token authentication.

Two tokens are used:
  - UPLOAD_TOKEN  — general API token for most protected endpoints
  - OVERLAND_TOKEN — dedicated token for the Overland GPS app endpoints

Both are sourced from config/settings.py (i.e. the .env file).

Usage:
    from fastapi import Depends
    from auth import require_upload_token, verify_overland_token

    # Via decorator (no extra parameter in handler):
    @router.get("/endpoint", dependencies=[Depends(require_upload_token)])
    async def endpoint(): ...

    # Via signature (access token value inside handler):
    async def endpoint(_: None = Depends(require_upload_token)): ...
"""

from fastapi import HTTPException, Header, status  # type: ignore

from config.settings import settings


def require_upload_token(authorization: str = Header(None)) -> None:
    """FastAPI dependency: enforce the UPLOAD_TOKEN bearer check.

    Raises HTTP 401 if the Authorization header is absent or does not match
    'Bearer <UPLOAD_TOKEN>'.  If UPLOAD_TOKEN is not configured, every request
    passes (useful for local development without a .env file).
    """
    if settings.upload_token:
        if not authorization or authorization != f"Bearer {settings.upload_token}":
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Unauthorized",
            )


def verify_overland_token(authorization: str = Header(...)) -> None:
    """FastAPI dependency: enforce the OVERLAND_TOKEN bearer check.

    Used exclusively on Overland GPS app endpoints.  Raises HTTP 401 if the
    Authorization header is missing or does not match the expected token.
    """
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or token != settings.overland_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing bearer token",
        )
