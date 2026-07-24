"""Security primitives: password hashing (bcrypt) and JWT encode/decode.

The user service is the only place tokens are *created*. Both services can
*verify* them because they share JWT_SECRET.
"""
from datetime import datetime, timedelta, timezone

import bcrypt
import jwt

from app.core.config import settings


def hash_password(plain: str) -> str:
    """Hash a plaintext password with a per-password random salt."""
    return bcrypt.hashpw(plain.encode(), bcrypt.gensalt()).decode()


def verify_password(plain: str, hashed: str) -> bool:
    """Constant-time check of a plaintext password against a stored hash."""
    return bcrypt.checkpw(plain.encode(), hashed.encode())


def create_access_token(*, subject: str, role: str, email: str) -> str:
    """Create a signed JWT.

    The claims (sub, role, email) are what other services read to authorize
    requests without calling back here. `sub` is the user id as a string.
    """
    now = datetime.now(timezone.utc)
    payload = {
        "sub": subject,
        "role": role,
        "email": email,
        "iat": now,
        "exp": now + timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES),
    }
    return jwt.encode(payload, settings.JWT_SECRET, algorithm=settings.JWT_ALGORITHM)


def decode_access_token(token: str) -> dict:
    """Decode and validate a JWT. Raises jwt.PyJWTError on any problem."""
    return jwt.decode(token, settings.JWT_SECRET, algorithms=[settings.JWT_ALGORITHM])
