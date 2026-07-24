"""Service layer: business logic sits here, not in the route handlers.

Routes stay thin (parse request, call service, shape response). This layer is
where rules like 'email must be unique' live, and it is what you would unit-test.
"""
import uuid

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.security import hash_password, verify_password
from app.models.user import User
from app.schemas.user import UserCreate, UserUpdate


class EmailAlreadyExistsError(Exception):
    """Raised when trying to register an email that is already taken."""


class UserNotFoundError(Exception):
    """Raised when a user id does not exist."""


def create_user(db: Session, data: UserCreate) -> User:
    existing = db.scalar(select(User).where(User.email == data.email))
    if existing is not None:
        raise EmailAlreadyExistsError(data.email)

    user = User(
        email=data.email,
        hashed_password=hash_password(data.password),
        full_name=data.full_name,
        role=data.role,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def get_user(db: Session, user_id: uuid.UUID) -> User:
    user = db.get(User, user_id)
    if user is None:
        raise UserNotFoundError(str(user_id))
    return user


def list_users(db: Session, *, limit: int = 50, offset: int = 0) -> list[User]:
    stmt = select(User).order_by(User.created_at.desc()).limit(limit).offset(offset)
    return list(db.scalars(stmt).all())


def update_user(db: Session, user_id: uuid.UUID, data: UserUpdate) -> User:
    user = get_user(db, user_id)
    if data.full_name is not None:
        user.full_name = data.full_name
    if data.is_active is not None:
        user.is_active = data.is_active
    db.commit()
    db.refresh(user)
    return user


def authenticate(db: Session, email: str, password: str) -> User | None:
    user = db.scalar(select(User).where(User.email == email))
    if user is None or not verify_password(password, user.hashed_password):
        return None
    return user
