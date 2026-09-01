"""Enrollment business logic."""
import uuid

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.core.config import settings
from app.models.enrollment import Enrollment


class EnrollmentAlreadyExistsError(Exception):
    pass


class EnrollmentNotFoundError(Exception):
    pass


class CourseFullError(Exception):
    pass


def count_enrollments_for_course(db: Session, course_id: uuid.UUID) -> int:
    stmt = select(func.count(Enrollment.id)).where(Enrollment.course_id == course_id)
    return db.scalar(stmt) or 0


def exists_for(db: Session, student_id: uuid.UUID, course_id: uuid.UUID) -> bool:
    """True if this student is already enrolled in this course. Used for the
    fast pre-check in the enroll endpoint before starting the workflow."""
    stmt = select(func.count(Enrollment.id)).where(
        Enrollment.student_id == student_id, Enrollment.course_id == course_id
    )
    return (db.scalar(stmt) or 0) > 0


def create_enrollment(
    db: Session, student_id: uuid.UUID, course_id: uuid.UUID
) -> Enrollment:
    if count_enrollments_for_course(db, course_id) >= settings.MAX_ENROLLMENTS_PER_COURSE:
        raise CourseFullError(str(course_id))

    enrollment = Enrollment(student_id=student_id, course_id=course_id)
    db.add(enrollment)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise EnrollmentAlreadyExistsError(f"{student_id}:{course_id}")
    db.refresh(enrollment)
    return enrollment


def create_enrollment_with_id(
    db: Session,
    enrollment_id: uuid.UUID,
    student_id: uuid.UUID,
    course_id: uuid.UUID,
) -> Enrollment:
    """Create an enrollment with a caller-supplied id. Used by the Temporal
    workflow's record_enrollment activity so the id is known before the row is
    written. Idempotent: if this id already exists (a retried activity), returns
    the existing row instead of erroring."""
    existing = db.get(Enrollment, enrollment_id)
    if existing is not None:
        return existing  # activity retry — already recorded

    if count_enrollments_for_course(db, course_id) >= settings.MAX_ENROLLMENTS_PER_COURSE:
        raise CourseFullError(str(course_id))

    enrollment = Enrollment(id=enrollment_id, student_id=student_id, course_id=course_id)
    db.add(enrollment)
    try:
        db.commit()
    except IntegrityError:
        # unique(student_id, course_id) — this student is already enrolled.
        db.rollback()
        raise EnrollmentAlreadyExistsError(f"{student_id}:{course_id}")
    db.refresh(enrollment)
    return enrollment


def list_enrollments(db: Session, student_id: uuid.UUID) -> list[Enrollment]:
    stmt = (
        select(Enrollment)
        .where(Enrollment.student_id == student_id)
        .order_by(Enrollment.enrolled_at.desc())
    )
    return list(db.scalars(stmt).all())


def get_enrollment(db: Session, enrollment_id: uuid.UUID) -> Enrollment:
    enrollment = db.get(Enrollment, enrollment_id)
    if enrollment is None:
        raise EnrollmentNotFoundError(str(enrollment_id))
    return enrollment


def delete_enrollment(db: Session, enrollment_id: uuid.UUID) -> None:
    enrollment = db.get(Enrollment, enrollment_id)
    if enrollment is None:
        return  # idempotent: compensation on a row that never got created
    db.delete(enrollment)
    db.commit()
