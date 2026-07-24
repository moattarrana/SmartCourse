"""Course service business logic."""
import uuid

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.course import Course, CourseStatus
from app.schemas.course import CourseCreate, CourseUpdate


class CourseNotFoundError(Exception):
    pass


def create_course(db: Session, data: CourseCreate, instructor_id: uuid.UUID) -> Course:
    course = Course(
        title=data.title,
        description=data.description,
        instructor_id=instructor_id,
        status=CourseStatus.DRAFT,
    )
    db.add(course)
    db.commit()
    db.refresh(course)
    return course


def get_course(db: Session, course_id: uuid.UUID) -> Course:
    course = db.get(Course, course_id)
    if course is None:
        raise CourseNotFoundError(str(course_id))
    return course


def list_courses(
    db: Session,
    *,
    status: CourseStatus | None = None,
    instructor_id: uuid.UUID | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[Course]:
    stmt = select(Course)
    if status is not None:
        stmt = stmt.where(Course.status == status)
    if instructor_id is not None:
        stmt = stmt.where(Course.instructor_id == instructor_id)
    stmt = stmt.order_by(Course.created_at.desc()).limit(limit).offset(offset)
    return list(db.scalars(stmt).all())


def update_course(db: Session, course_id: uuid.UUID, data: CourseUpdate) -> Course:
    course = get_course(db, course_id)
    if data.title is not None:
        course.title = data.title
    if data.description is not None:
        course.description = data.description
    if data.status is not None:
        course.status = data.status
    db.commit()
    db.refresh(course)
    return course


def delete_course(db: Session, course_id: uuid.UUID) -> None:
    course = get_course(db, course_id)
    db.delete(course)
    db.commit()
