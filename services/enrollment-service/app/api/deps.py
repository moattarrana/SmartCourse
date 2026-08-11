"""Auth dependencies for the enrollment service. Identity comes from the JWT."""
import enum
import uuid

import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.core.security import decode_access_token

_bearer = HTTPBearer(auto_error=True)


class Role(str, enum.Enum):
    STUDENT = "student"
    INSTRUCTOR = "instructor"


class CurrentUser:
    def __init__(self, user_id: uuid.UUID, role: Role, email: str):
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
            role=Role(payload["role"]),
            email=payload["email"],
        )
    except (jwt.PyJWTError, KeyError, ValueError):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )


def get_bearer_token(creds: HTTPAuthorizationCredentials = Depends(_bearer)) -> str:
    """Raw token, forwarded to the course service for course validation."""
    return creds.credentials


def require_student(current: CurrentUser = Depends(get_current_user)) -> CurrentUser:
    if current.role != Role.STUDENT:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only students can enroll",
        )
    return current
