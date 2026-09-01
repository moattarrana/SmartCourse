"""Internal-only endpoints for the Temporal worker's EnrollmentWorkflow.

The worker orchestrates enrollment but must NOT touch enrollments_db directly,
so it drives the writes through these endpoints. Guarded by a shared secret
(X-Internal-Key); enrollment-service remains the sole owner of its database.
Mirrors course-service's internal router.
"""
import logging
import uuid

from fastapi import APIRouter, Depends, Header, HTTPException, status
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.core.config import settings
from app.core.database import get_db
from app.events.enrollment_events import publish_student_enrolled
from app.services import enrollment_service, progress_service

logger = logging.getLogger(settings.SERVICE_NAME)

router = APIRouter(prefix="/internal/enrollments", tags=["internal"])


def require_internal_key(x_internal_key: str = Header(default="")) -> None:
    if not settings.INTERNAL_API_KEY or x_internal_key != settings.INTERNAL_API_KEY:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Invalid internal key"
        )


# ---- Request / Response models ----
class EnrollmentWork(BaseModel):
    enrollment_id: uuid.UUID
    student_id: uuid.UUID
    course_id: uuid.UUID


class OkResponse(BaseModel):
    status: str
    enrollment_id: uuid.UUID


@router.post("/record", response_model=OkResponse)
def record_enrollment(
    body: EnrollmentWork,
    _: None = Depends(require_internal_key),
    db: Session = Depends(get_db),
) -> OkResponse:
    """Create the enrollment row with the workflow-supplied id (idempotent)."""
    try:
        enrollment_service.create_enrollment_with_id(
            db, body.enrollment_id, body.student_id, body.course_id
        )
    except enrollment_service.CourseFullError:
        raise HTTPException(status_code=409, detail="Course is full")
    except enrollment_service.EnrollmentAlreadyExistsError:
        raise HTTPException(status_code=409, detail="Already enrolled in this course")
    return OkResponse(status="recorded", enrollment_id=body.enrollment_id)


@router.post("/progress", response_model=OkResponse)
def init_progress(
    body: EnrollmentWork,
    _: None = Depends(require_internal_key),
    db: Session = Depends(get_db),
) -> OkResponse:
    """Initialize progress tracking for the enrollment (idempotent)."""
    progress_service.init_progress(
        db, body.enrollment_id, body.student_id, body.course_id
    )
    return OkResponse(status="progress_initialized", enrollment_id=body.enrollment_id)


@router.post("/emit-enrolled", response_model=OkResponse)
def emit_enrolled(
    body: EnrollmentWork,
    _: None = Depends(require_internal_key),
) -> OkResponse:
    """Publish the StudentEnrolled event to Kafka (analytics + notifications
    consume it downstream — unchanged)."""
    publish_student_enrolled(
        enrollment_id=body.enrollment_id,
        student_id=body.student_id,
        course_id=body.course_id,
    )
    return OkResponse(status="event_emitted", enrollment_id=body.enrollment_id)


@router.post("/rollback", response_model=OkResponse)
def rollback_enrollment(
    body: EnrollmentWork,
    _: None = Depends(require_internal_key),
    db: Session = Depends(get_db),
) -> OkResponse:
    """Compensation: remove the enrollment row if the workflow failed."""
    enrollment_service.delete_enrollment(db, body.enrollment_id)
    return OkResponse(status="rolled_back", enrollment_id=body.enrollment_id)
