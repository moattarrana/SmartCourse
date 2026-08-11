"""Enrollment business logic."""
import uuid

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models.enrollment import Enrollment


class EnrollmentAlreadyExistsError(Exception):
    pass


class EnrollmentNotFoundError(Exception):
    pass


def create_enrollment(
    db: Session, student_id: uuid.UUID, course_id: uuid.UUID
) -> Enrollment:
    enrollment = Enrollment(student_id=student_id, course_id=course_id)
    db.add(enrollment)
    try:
        db.commit()
    except IntegrityError:
        # The unique(student_id, course_id) constraint rejected a duplicate.
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
    enrollment = get_enrollment(db, enrollment_id)
    db.delete(enrollment)
    db.commit()
