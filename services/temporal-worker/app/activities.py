"""Activities: the steps that touch the database. Registered by name so the
workflow can reference them as strings and stay import-light/deterministic."""
import time
import uuid

from sqlalchemy import text
from temporalio import activity
from temporalio.exceptions import ApplicationError

from app.db import Course, CourseStatus, SessionLocal


@activity.defn(name="validate_course")
def validate_course(course_id: str) -> None:
    cid = uuid.UUID(course_id)
    db = SessionLocal()
    try:
        course = db.get(Course, cid)
        if course is None:
            raise ApplicationError("Course not found", non_retryable=True)
        if course.status != CourseStatus.PUBLISHING:
            raise ApplicationError(
                f"Course is {course.status.value}, expected publishing",
                non_retryable=True,
            )
        n = db.execute(
            text(
                "SELECT count(l.id) FROM lessons l "
                "JOIN modules m ON l.module_id = m.id WHERE m.course_id = :cid"
            ),
            {"cid": cid},
        ).scalar() or 0
        if n == 0:
            raise ApplicationError("Course has no lessons", non_retryable=True)
        activity.logger.info("Course %s validated", course_id)
    finally:
        db.close()


@activity.defn(name="process_content")
def process_content(course_id: str) -> None:
    """Week 2 stub (real chunking/embeddings arrive in Part B). Simulated work
    so retries/timeouts are demonstrable."""
    activity.logger.info("Processing content for course %s", course_id)
    time.sleep(2)

#to check failed scenario
# @activity.defn(name="process_content")
# def process_content(course_id: str) -> None:
#     raise RuntimeError("simulated content-processing failure")

@activity.defn(name="mark_published")
def mark_published(course_id: str) -> None:
    cid = uuid.UUID(course_id)
    db = SessionLocal()
    try:
        course = db.get(Course, cid)
        if course is not None:
            course.status = CourseStatus.PUBLISHED
            db.commit()
        activity.logger.info("Course %s published", course_id)
    finally:
        db.close()


@activity.defn(name="mark_publish_failed")
def mark_publish_failed(course_id: str) -> None:
    """Compensation: return a stuck PUBLISHING course to DRAFT."""
    cid = uuid.UUID(course_id)
    db = SessionLocal()
    try:
        course = db.get(Course, cid)
        if course is not None and course.status == CourseStatus.PUBLISHING:
            course.status = CourseStatus.DRAFT
            db.commit()
        activity.logger.info("Course %s rolled back to draft", course_id)
    finally:
        db.close()
