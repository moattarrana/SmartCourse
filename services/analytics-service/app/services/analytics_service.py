"""Analytics read/write logic over MongoDB.

Idempotency: every event carries an event_id. We insert it into the
processed_events ledger first; a DuplicateKeyError means we've already handled
it (Kafka is at-least-once), so we skip. This prevents double-counting.
"""
import logging
from datetime import datetime, timezone

from pymongo.errors import DuplicateKeyError

from app.core.config import settings
from app.core import db

logger = logging.getLogger(settings.SERVICE_NAME)


def _already_processed(event_id: str, event_type: str) -> bool:
    try:
        db.processed_events().insert_one(
            {"_id": event_id, "type": event_type, "at": datetime.now(timezone.utc)}
        )
        return False
    except DuplicateKeyError:
        return True


def _counter(name: str, default=0):
    doc = db.counters().find_one({"_id": name})
    return doc["value"] if doc else default


def _day_of(iso_ts: str | None) -> str:
    try:
        return datetime.fromisoformat(iso_ts).astimezone(timezone.utc).date().isoformat()
    except Exception:
        return datetime.now(timezone.utc).date().isoformat()


def record_user_registered(event: dict) -> None:
    event_id = event.get("event_id")
    role = event.get("role")
    if not event_id or not role:
        logger.warning("Skipping malformed UserRegistered event: %s", event)
        return
    if _already_processed(event_id, event.get("event_type", "UserRegistered")):
        logger.info("Duplicate event %s ignored", event_id)
        return

    if role == "student":
        db.counters().update_one(
            {"_id": "total_students"}, {"$inc": {"value": 1}}, upsert=True
        )
    elif role == "instructor":
        db.counters().update_one(
            {"_id": "total_instructors"}, {"$inc": {"value": 1}}, upsert=True
        )
    elif role == "admin":
        db.counters().update_one(
            {"_id": "total_admins"}, {"$inc": {"value": 1}}, upsert=True
        )
    logger.info("Recorded user registered role=%s (event %s)", role, event_id)


def record_student_enrolled(event: dict) -> None:
    event_id = event.get("event_id")
    course_id = event.get("course_id")
    student_id = event.get("student_id")
    if not event_id or not course_id:
        logger.warning("Skipping malformed StudentEnrolled event: %s", event)
        return
    if _already_processed(event_id, event.get("event_type", "StudentEnrolled")):
        logger.info("Duplicate event %s ignored", event_id)
        return

    db.counters().update_one(
        {"_id": "total_enrollments"}, {"$inc": {"value": 1}}, upsert=True
    )
    db.course_enrollments().update_one(
        {"_id": course_id}, {"$inc": {"count": 1}}, upsert=True
    )
    if student_id:
        db.get_db()["students"].update_one(
            {"_id": student_id}, {"$set": {"seen": True}}, upsert=True
        )
    day = _day_of(event.get("occurred_at"))
    db.get_db()["enrollments_by_day"].update_one(
        {"_id": day}, {"$inc": {"count": 1}}, upsert=True
    )
    logger.info("Recorded enrollment for course %s (event %s)", course_id, event_id)


def record_course_published(event: dict) -> None:
    event_id = event.get("event_id")
    course_id = event.get("course_id")
    if not event_id or not course_id:
        logger.warning("Skipping malformed CoursePublished event: %s", event)
        return
    if _already_processed(event_id, event.get("event_type", "CoursePublished")):
        logger.info("Duplicate event %s ignored", event_id)
        return

    db.counters().update_one(
        {"_id": "total_courses_published"}, {"$inc": {"value": 1}}, upsert=True
    )
    logger.info("Recorded course published %s (event %s)", course_id, event_id)


def record_course_completed(event: dict) -> None:
    event_id = event.get("event_id")
    course_id = event.get("course_id")
    if not event_id or not course_id:
        logger.warning("Skipping malformed CourseCompleted event: %s", event)
        return
    if _already_processed(event_id, event.get("event_type", "CourseCompleted")):
        logger.info("Duplicate event %s ignored", event_id)
        return

    db.counters().update_one(
        {"_id": "total_completions"}, {"$inc": {"value": 1}}, upsert=True
    )
    started, completed = event.get("started_at"), event.get("completed_at")
    if started and completed:
        try:
            secs = (
                datetime.fromisoformat(completed) - datetime.fromisoformat(started)
            ).total_seconds()
            if secs >= 0:
                db.counters().update_one(
                    {"_id": "sum_completion_seconds"},
                    {"$inc": {"value": secs}},
                    upsert=True,
                )
                db.counters().update_one(
                    {"_id": "count_timed_completions"},
                    {"$inc": {"value": 1}},
                    upsert=True,
                )
        except Exception as exc:
            logger.warning("Could not compute completion duration: %s", exc)
    logger.info("Recorded completion for course %s (event %s)", course_id, event_id)


def increment_failed_events() -> None:
    db.counters().update_one(
        {"_id": "failed_events"}, {"$inc": {"value": 1}}, upsert=True
    )


def get_analytics() -> dict:
    total_enrollments = _counter("total_enrollments")
    total_completions = _counter("total_completions")
    distinct_students = db.get_db()["students"].count_documents({})

    completion_rate = (
        round(total_completions / total_enrollments, 4) if total_enrollments else 0.0
    )

    sum_secs = _counter("sum_completion_seconds", 0.0)
    timed = _counter("count_timed_completions")
    avg_secs = round(sum_secs / timed, 2) if timed else None
    avg_hours = round(avg_secs / 3600, 2) if avg_secs is not None else None

    avg_courses_per_student = (
        round(total_enrollments / distinct_students, 2) if distinct_students else 0.0
    )

    top_courses = [
        {"course_id": d["_id"], "enrollments": d["count"]}
        for d in db.course_enrollments().find().sort("count", -1).limit(10)
    ]
    enrollments_over_time = [
        {"date": d["_id"], "count": d["count"]}
        for d in db.get_db()["enrollments_by_day"].find().sort("_id", 1)
    ]

    return {
        "total_students": _counter("total_students"),
        "total_instructors": _counter("total_instructors"),
        "total_courses_published": _counter("total_courses_published"),
        "total_enrollments": total_enrollments,
        "total_completions": total_completions,
        "course_completion_rate": completion_rate,
        "average_time_to_complete_seconds": avg_secs,
        "average_time_to_complete_hours": avg_hours,
        "new_enrollments_over_time": enrollments_over_time,
        "most_popular_courses": top_courses,
        "distinct_students": distinct_students,
        "average_courses_per_student": avg_courses_per_student,
        "failed_events": _counter("failed_events"),
        "events_processed": db.processed_events().count_documents({}),
        "notifications_sent": db.notifications().count_documents({}),
    }
