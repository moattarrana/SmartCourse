"""Course endpoints. Reads are open to any authenticated user; writes require
the instructor role, and edits/deletes require ownership."""
import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.api.deps import CurrentUser, get_current_user, require_instructor
from app.core.config import settings
from app.core.database import get_db
from app.core.temporal_client import get_temporal_client
from app.models.course import CourseStatus
from app.schemas.course import CourseCreate, CourseRead, CourseUpdate
from app.services import content_service, course_service

try:  # exception location varies slightly across temporalio versions
    from temporalio.client import WorkflowAlreadyStartedError
except ImportError:  # pragma: no cover
    from temporalio.exceptions import WorkflowAlreadyStartedError

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
    try:
        course = course_service.update_course(db, course_id, data)
    except course_service.InvalidTransitionError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Illegal status transition: {exc}",
        )
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


@router.post("/{course_id}/publish", status_code=status.HTTP_202_ACCEPTED)
async def publish_course(
    course_id: uuid.UUID,
    current: CurrentUser = Depends(require_instructor),
    db: Session = Depends(get_db),
) -> dict:
    """Start the Temporal publishing workflow for a course.

    Validates ownership, state, and that the course has content, flips it to
    PUBLISHING, then hands off to Temporal. The worker moves it to PUBLISHED on
    success or back to DRAFT on failure.
    """
    try:
        course = course_service.get_course(db, course_id)
    except course_service.CourseNotFoundError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Course not found")
    _ensure_owner(course, current)

    if course.status == CourseStatus.PUBLISHED:
        raise HTTPException(status_code=409, detail="Course already published")
    if course.status == CourseStatus.PUBLISHING:
        raise HTTPException(status_code=409, detail="Publish already in progress")
    try:
        course_service.assert_can_transition(course.status, CourseStatus.PUBLISHING)
    except course_service.InvalidTransitionError:
        raise HTTPException(
            status_code=409, detail=f"Cannot publish from {course.status.value}"
        )
# if course doesnt have modules or lessions
    if not content_service.course_has_content(db, course_id):
        raise HTTPException(status_code=422, detail="Course has no lessons to publish")

    # Flip to PUBLISHING first; the workflow owns the rest of the transition.
    course_service.set_status(db, course_id, CourseStatus.PUBLISHING)

    client = await get_temporal_client()
    try:
        #idempotency: if a publish is already running for that course, starting again raises
        await client.start_workflow(
            "CoursePublishingWorkflow",
            str(course_id),
            id=f"publish-{course_id}",
            task_queue=settings.TEMPORAL_TASK_QUEUE,
        )
    except WorkflowAlreadyStartedError:
        pass  # idempotent: a publish for this course is already running
    except Exception:
        # Temporal unreachable — undo the state change so it isn't stuck.
        course_service.set_status(db, course_id, CourseStatus.DRAFT)
        raise HTTPException(status_code=503, detail="Publishing service unavailable")

    return {"course_id": str(course_id), "status": "publishing"}
