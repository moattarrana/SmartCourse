"""Enrollment endpoints.

Enrolling is orchestrated by Temporal: the endpoint does fast pre-checks
(course enrollable, duplicate -> 409, capacity -> 409), then starts the
EnrollmentWorkflow, which durably records the enrollment, initializes progress,
and emits the StudentEnrolled event (Kafka/Celery handle analytics +
notifications downstream). Mirrors how course publishing starts its workflow.
"""
import logging
import uuid

import httpx
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.api.deps import CurrentUser, get_bearer_token, get_current_user, require_student
from app.core.config import settings
from app.core.database import get_db
from app.core.temporal_client import get_temporal_client
from app.events.progress_events import publish_course_completed
from app.schemas.enrollment import EnrollmentCreate, EnrollmentRead
from app.schemas.progress import ProgressRead, ProgressUpdate
from app.services import course_client, enrollment_service, progress_service

try:  # exception location varies slightly across temporalio versions
    from temporalio.client import WorkflowAlreadyStartedError
except ImportError:  # pragma: no cover
    from temporalio.exceptions import WorkflowAlreadyStartedError

logger = logging.getLogger(settings.SERVICE_NAME)

router = APIRouter(prefix="/enrollments", tags=["enrollments"])


def _ensure_owner(enrollment, current: CurrentUser) -> None:
    if enrollment.student_id != current.id:
        raise HTTPException(status_code=403, detail="Not your enrollment")


def _load_owned_enrollment(db: Session, enrollment_id: uuid.UUID, current: CurrentUser):
    try:
        enrollment = enrollment_service.get_enrollment(db, enrollment_id)
    except enrollment_service.EnrollmentNotFoundError:
        raise HTTPException(status_code=404, detail="Enrollment not found")
    _ensure_owner(enrollment, current)
    return enrollment


@router.post("", status_code=status.HTTP_202_ACCEPTED)
async def enroll(
    data: EnrollmentCreate,
    token: str = Depends(get_bearer_token),
    current: CurrentUser = Depends(require_student),
    db: Session = Depends(get_db),
) -> dict:
    """Start the Temporal EnrollmentWorkflow for a student + course.

    Fast pre-checks run synchronously so the caller gets immediate feedback for
    the common rejections; the durable work (record, progress, emit) runs in the
    workflow. Returns 202 with the enrollment_id; poll GET /enrollments/{id}.
    """
    # 1) Course must exist and be published (course-service).
    try:
        #The course lives in another service, so enrollment-service can't check it locally 
        # — it has to ask course-service over HTTP.
        await course_client.assert_course_enrollable(token, data.course_id) 
    except course_client.CourseNotFoundError:
        raise HTTPException(status_code=404, detail="Course not found")
    except course_client.CourseNotPublishedError:
        raise HTTPException(status_code=400, detail="Course is not open for enrollment")
    except httpx.HTTPError:
        raise HTTPException(status_code=503, detail="Course service unavailable")

    # 2) Fast pre-checks for immediate feedback (the workflow's record step is
    #    the authoritative backstop via the DB unique constraint + capacity).
    if enrollment_service.exists_for(db, current.id, data.course_id):
        raise HTTPException(status_code=409, detail="Already enrolled in this course")
    if (
        enrollment_service.count_enrollments_for_course(db, data.course_id)
        >= settings.MAX_ENROLLMENTS_PER_COURSE
    ):
        raise HTTPException(status_code=409, detail="Course is full")

    # 3) Mint the enrollment id up front so we can return it now and the
    #    workflow can create the row with a known id.
    enrollment_id = uuid.uuid4()

    # 4) Start the workflow (idempotent by workflow id).
    client = await get_temporal_client()
    try:
        await client.start_workflow(
            "EnrollmentWorkflow",
            args=[str(enrollment_id), str(current.id), str(data.course_id)],
            id=f"enroll-{enrollment_id}", #workflow's unique name.
            task_queue=settings.TEMPORAL_TASK_QUEUE,
        )
    except WorkflowAlreadyStartedError:
        pass  # idempotent
    except Exception:
        raise HTTPException(status_code=503, detail="Enrollment service unavailable")

    return {"enrollment_id": str(enrollment_id), "status": "processing"}


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
    enrollment = _load_owned_enrollment(db, enrollment_id, current)
    return EnrollmentRead.model_validate(enrollment)


@router.delete("/{enrollment_id}", status_code=status.HTTP_204_NO_CONTENT)
def unenroll(
    enrollment_id: uuid.UUID,
    current: CurrentUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> None:
    _load_owned_enrollment(db, enrollment_id, current)
    enrollment_service.delete_enrollment(db, enrollment_id)


# ---- Progress ----
@router.get("/{enrollment_id}/progress", response_model=ProgressRead)
def get_progress(
    enrollment_id: uuid.UUID,
    current: CurrentUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> ProgressRead:
    _load_owned_enrollment(db, enrollment_id, current)
    try:
        progress = progress_service.get_by_enrollment(db, enrollment_id)
    except progress_service.ProgressNotFoundError:
        raise HTTPException(status_code=404, detail="Progress not found")
    return ProgressRead.model_validate(progress)


@router.patch("/{enrollment_id}/progress", response_model=ProgressRead)
def update_progress(
    enrollment_id: uuid.UUID,
    data: ProgressUpdate,
    current: CurrentUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> ProgressRead:
    _load_owned_enrollment(db, enrollment_id, current) #the enrollment must exist and belong to the caller
    try:
        progress, just_completed = progress_service.update_progress(
            db, enrollment_id, data.percent
        )
    except progress_service.ProgressNotFoundError:
        raise HTTPException(status_code=404, detail="Progress not found")

    if just_completed:   #Only when the student just crossed into completed do we publish the CourseCompleted event to Kafka.
        publish_course_completed(
            enrollment_id=progress.enrollment_id,
            student_id=progress.student_id,
            course_id=progress.course_id,
            started_at=progress.started_at,
            completed_at=progress.completed_at,
        )

    return ProgressRead.model_validate(progress)
