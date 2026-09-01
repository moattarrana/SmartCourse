"""Internal-only endpoints for the Temporal worker.

The worker orchestrates publishing but must NOT touch courses_db directly, so
it drives state changes through these endpoints. They are guarded by a shared
secret (X-Internal-Key) so ordinary users cannot call them, and course-service
remains the sole owner of its database.

Note: CoursePublished is now published to Kafka by the Temporal worker as an
orchestrated workflow step, so this endpoint no longer emits it.
"""
import uuid

from fastapi import APIRouter, Depends, Header, HTTPException, status
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.core.config import settings
from app.core.database import get_db
from app.models.course import CourseStatus
from app.services import content_service, course_service

router = APIRouter(prefix="/courses/{course_id}/internal", tags=["internal"])


def require_internal_key(x_internal_key: str = Header(default="")) -> None:
    if not settings.INTERNAL_API_KEY or x_internal_key != settings.INTERNAL_API_KEY:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Invalid internal key"
        )


# ---- Request / Response models ----
class StatusUpdate(BaseModel):
    status: CourseStatus


class PublishCheckResponse(BaseModel):
    course_id: uuid.UUID
    status: CourseStatus
    has_content: bool


class CourseStatusResponse(BaseModel):
    course_id: uuid.UUID
    status: CourseStatus


@router.get("/publish-check", response_model=PublishCheckResponse)
def publish_check(
    course_id: uuid.UUID,
    _: None = Depends(require_internal_key),
    db: Session = Depends(get_db),
) -> PublishCheckResponse:
    """Facts the worker's validate step needs, without it touching the DB."""
    try:
        course = course_service.get_course(db, course_id)
    except course_service.CourseNotFoundError:
        raise HTTPException(status_code=404, detail="Course not found")
    return PublishCheckResponse(
        course_id=course_id,
        status=course.status,
        has_content=content_service.course_has_content(db, course_id),
    )


@router.post("/status", response_model=CourseStatusResponse)
def set_course_status(
    course_id: uuid.UUID,
    body: StatusUpdate,
    _: None = Depends(require_internal_key),
    db: Session = Depends(get_db),
) -> CourseStatusResponse:
    """Apply a validated status transition on behalf of the workflow.

    The workflow publishes CoursePublished itself after this returns, so no event
    is emitted here.
    """
    try:
        course = course_service.set_status(db, course_id, body.status)
    except course_service.CourseNotFoundError:
        raise HTTPException(status_code=404, detail="Course not found")
    except course_service.InvalidTransitionError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Illegal status transition: {exc}",
        )

    return CourseStatusResponse(course_id=course_id, status=course.status)
