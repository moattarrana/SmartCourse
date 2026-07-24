"""User endpoints: read own profile, look up, list, update."""
import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.api.deps import CurrentUser, get_current_user
from app.core.database import get_db
from app.schemas.user import UserRead, UserUpdate
from app.services import user_service

router = APIRouter(prefix="/users", tags=["users"])


@router.get("/me", response_model=UserRead)
def read_me(
    current: CurrentUser = Depends(get_current_user), db: Session = Depends(get_db)
) -> UserRead:
    user = user_service.get_user(db, current.id)
    return UserRead.model_validate(user)


@router.get("", response_model=list[UserRead])
def list_users(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    _: CurrentUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> list[UserRead]:
    users = user_service.list_users(db, limit=limit, offset=offset)
    return [UserRead.model_validate(u) for u in users]


@router.get("/{user_id}", response_model=UserRead)
def get_user(
    user_id: uuid.UUID,
    current: CurrentUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> UserRead:
    try:
        user = user_service.get_user(db, user_id)
    except user_service.UserNotFoundError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    return UserRead.model_validate(user)


@router.patch("/{user_id}", response_model=UserRead)
def update_user(
    user_id: uuid.UUID,
    data: UserUpdate,
    current: CurrentUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> UserRead:
    # A user may edit only themselves.
    if current.id != user_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Cannot modify another user"
        )
    try:
        user = user_service.update_user(db, user_id, data)
    except user_service.UserNotFoundError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    return UserRead.model_validate(user)
