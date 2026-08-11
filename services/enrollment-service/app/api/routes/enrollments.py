"""Enrollment endpoints. Students enroll once per course (duplicates -> 409),
and only in courses that exist and are published."""
import uuid

import httpx
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.api.deps import CurrentUser, get_bearer_token, get_current_user, require_student
from app.core.database import get_db
from app.schemas.enrollment import EnrollmentCreate, EnrollmentRead
from app.services import course_client, enrollment_service

router = APIRouter(prefix="/enrollments", tags=["enrollments"])


def _ensure_owner(enrollment, current: CurrentUser) -> None:
    if enrollment.student_id != current.id:
        raise HTTPException(status_code=403, detail="Not your enrollment")


@router.post("", response_model=EnrollmentRead, status_code=status.HTTP_201_CREATED)
async def enroll(
    data: EnrollmentCreate,
    token: str = Depends(get_bearer_token),
    current: CurrentUser = Depends(require_student),
    db: Session = Depends(get_db),
) -> EnrollmentRead:
    try:
        await course_client.assert_course_enrollable(token, data.course_id)
    except course_client.CourseNotFoundError:
        raise HTTPException(status_code=404, detail="Course not found")
    except course_client.CourseNotPublishedError:
        raise HTTPException(status_code=400, detail="Course is not open for enrollment")
    except httpx.HTTPError:
        raise HTTPException(status_code=503, detail="Course service unavailable")

    try:
        enrollment = enrollment_service.create_enrollment(db, current.id, data.course_id)
    except enrollment_service.EnrollmentAlreadyExistsError:
        raise HTTPException(status_code=409, detail="Already enrolled in this course")
    return EnrollmentRead.model_validate(enrollment)


@router.get("", response_model=list[EnrollmentRead])
def list_my_enrollments(
    current: CurrentUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> list[EnrollmentRead]:
    return [
        EnrollmentRead.model_validate(e)
        for e in enrollment_service.list_enrollments(db, current.id)
    ]


@router.get("/{enrollment_id}", response_model=EnrollmentRead)
def get_enrollment(
    enrollment_id: uuid.UUID,
    current: CurrentUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> EnrollmentRead:
    try:
        enrollment = enrollment_service.get_enrollment(db, enrollment_id)
    except enrollment_service.EnrollmentNotFoundError:
        raise HTTPException(status_code=404, detail="Enrollment not found")
    _ensure_owner(enrollment, current)
    return EnrollmentRead.model_validate(enrollment)


@router.delete("/{enrollment_id}", status_code=status.HTTP_204_NO_CONTENT)
def unenroll(
    enrollment_id: uuid.UUID,
    current: CurrentUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> None:
    try:
        enrollment = enrollment_service.get_enrollment(db, enrollment_id)
    except enrollment_service.EnrollmentNotFoundError:
        raise HTTPException(status_code=404, detail="Enrollment not found")
    _ensure_owner(enrollment, current)
    enrollment_service.delete_enrollment(db, enrollment_id)
