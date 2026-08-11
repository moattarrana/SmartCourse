"""JWT verification only — this service does not issue tokens."""
import jwt

from app.core.config import settings


def decode_access_token(token: str) -> dict:
    return jwt.decode(token, settings.JWT_SECRET, algorithms=[settings.JWT_ALGORITHM])
