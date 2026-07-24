"""Reusable FastAPI dependencies for auth and authorization."""
import uuid

import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.core.security import decode_access_token
from app.models.user import UserRole

_bearer = HTTPBearer(auto_error=True)


class CurrentUser:
    """Lightweight identity extracted from the JWT (no DB hit required)."""

    def __init__(self, user_id: uuid.UUID, role: UserRole, email: str):
        self.id = user_id
        self.role = role
        self.email = email


def get_current_user(
    creds: HTTPAuthorizationCredentials = Depends(_bearer),
) -> CurrentUser:
    try:
        payload = decode_access_token(creds.credentials)
        return CurrentUser(
            user_id=uuid.UUID(payload["sub"]),
            role=UserRole(payload["role"]),
            email=payload["email"],
        )
    except (jwt.PyJWTError, KeyError, ValueError):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )
