"""Progress business logic."""
import uuid
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models.progress import Progress, ProgressStatus


class ProgressNotFoundError(Exception):
    pass


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def init_progress(
    db: Session,
    enrollment_id: uuid.UUID,
    student_id: uuid.UUID,
    course_id: uuid.UUID,
) -> Progress | None:
    """Create the progress record for a new enrollment. Idempotent: if one
    already exists (unique enrollment_id), returns None without error."""
    progress = Progress(
        enrollment_id=enrollment_id,
        student_id=student_id,
        course_id=course_id,
        status=ProgressStatus.NOT_STARTED,
        percent=0,
    )
    db.add(progress)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        return None
    db.refresh(progress)
    return progress


def get_by_enrollment(db: Session, enrollment_id: uuid.UUID) -> Progress:
    stmt = select(Progress).where(Progress.enrollment_id == enrollment_id)
    progress = db.scalars(stmt).first()
    if progress is None:
        raise ProgressNotFoundError(str(enrollment_id))
    return progress


def update_progress(
    db: Session, enrollment_id: uuid.UUID, percent: int
) -> tuple[Progress, bool]:
    """Update percent and derive status. Returns (progress, just_completed)
    where just_completed is True only on the transition into COMPLETED."""
    progress = get_by_enrollment(db, enrollment_id)

    was_completed = progress.status == ProgressStatus.COMPLETED
    progress.percent = percent

    if percent <= 0:
        progress.status = ProgressStatus.NOT_STARTED
    elif percent >= 100:
        progress.percent = 100
        progress.status = ProgressStatus.COMPLETED
        if progress.completed_at is None:
            progress.completed_at = _utcnow()
    else:
        progress.status = ProgressStatus.IN_PROGRESS

    if progress.status != ProgressStatus.NOT_STARTED and progress.started_at is None:
        progress.started_at = _utcnow()

    db.commit()
    db.refresh(progress)

    just_completed = (progress.status == ProgressStatus.COMPLETED) and not was_completed
    return progress, just_completed
