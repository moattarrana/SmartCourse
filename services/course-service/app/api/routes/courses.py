"""Course endpoints. Reads are open to any authenticated user; writes require
the instructor role, and edits/deletes require ownership."""
import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.api.deps import CurrentUser, get_current_user, require_instructor
from app.core.database import get_db
from app.models.course import CourseStatus
from app.schemas.course import CourseCreate, CourseRead, CourseUpdate
from app.services import course_service

router = APIRouter(prefix="/courses", tags=["courses"])


def _ensure_owner(course, current: CurrentUser) -> None:
    if course.instructor_id != current.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not own this course",
        )


@router.post("", response_model=CourseRead, status_code=status.HTTP_201_CREATED)
def create_course(
    data: CourseCreate,
    current: CurrentUser = Depends(require_instructor),
    db: Session = Depends(get_db),
) -> CourseRead:
    course = course_service.create_course(db, data, instructor_id=current.id)
    return CourseRead.model_validate(course)


@router.get("", response_model=list[CourseRead])
def list_courses(
    status_filter: CourseStatus | None = Query(default=None, alias="status"),
    instructor_id: uuid.UUID | None = None,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    _: CurrentUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> list[CourseRead]:
    courses = course_service.list_courses(
        db, status=status_filter, instructor_id=instructor_id, limit=limit, offset=offset
    )
    return [CourseRead.model_validate(c) for c in courses]


@router.get("/{course_id}", response_model=CourseRead)
def get_course(
    course_id: uuid.UUID,
    _: CurrentUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> CourseRead:
    try:
        course = course_service.get_course(db, course_id)
    except course_service.CourseNotFoundError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Course not found")
    return CourseRead.model_validate(course)


@router.patch("/{course_id}", response_model=CourseRead)
def update_course(
    course_id: uuid.UUID,
    data: CourseUpdate,
    current: CurrentUser = Depends(require_instructor),
    db: Session = Depends(get_db),
) -> CourseRead:
    try:
        course = course_service.get_course(db, course_id)
    except course_service.CourseNotFoundError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Course not found")
    _ensure_owner(course, current)
    course = course_service.update_course(db, course_id, data)
    return CourseRead.model_validate(course)


@router.delete("/{course_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_course(
    course_id: uuid.UUID,
    current: CurrentUser = Depends(require_instructor),
    db: Session = Depends(get_db),
) -> None:
    try:
        course = course_service.get_course(db, course_id)
    except course_service.CourseNotFoundError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Course not found")
    _ensure_owner(course, current)
    course_service.delete_course(db, course_id)
