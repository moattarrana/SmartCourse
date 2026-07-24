"""JWT verification only — this service does not issue tokens."""
import jwt

from app.core.config import settings


def decode_access_token(token: str) -> dict:
    """Decode and validate a JWT issued by the user service."""
    return jwt.decode(token, settings.JWT_SECRET, algorithms=[settings.JWT_ALGORITHM])
