import hashlib
import hmac
from uuid import UUID

from fastapi import Depends, HTTPException, Request
from fastapi.security import APIKeyHeader, HTTPAuthorizationCredentials, HTTPBearer

from feedback_platform.db import fetchone
from feedback_platform.settings import get_settings

bearer = HTTPBearer(auto_error=False)
service_header = APIKeyHeader(name="X-Service-Token", auto_error=False)


def hash_key(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()


async def tenant_auth(
    request: Request, credentials: HTTPAuthorizationCredentials | None = Depends(bearer)
) -> UUID:
    if credentials is None or len(credentials.credentials) > 512:
        raise HTTPException(
            401, "Valid bearer API key required", headers={"WWW-Authenticate": "Bearer"}
        )
    async with request.app.state.pool.connection() as conn:
        row = await fetchone(
            conn,
            "SELECT tenant_id FROM api_keys WHERE key_hash=%s AND active=true",
            (hash_key(credentials.credentials),),
        )
    if row is None:
        raise HTTPException(401, "Invalid API key", headers={"WWW-Authenticate": "Bearer"})
    return row["tenant_id"]


async def service_auth(token: str | None = Depends(service_header)):
    secret = get_settings().internal_api_key
    if not secret or not token or not hmac.compare_digest(secret, token):
        raise HTTPException(401, "Valid service token required")
