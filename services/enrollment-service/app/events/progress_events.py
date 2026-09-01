"""Builders + publisher for progress domain events."""
import uuid
from datetime import datetime, timezone

from app.core.config import settings
from app.events.producer import publish_event

COURSE_COMPLETED = "CourseCompleted"


def publish_course_completed(
    enrollment_id: uuid.UUID,
    student_id: uuid.UUID,
    course_id: uuid.UUID,
    started_at: datetime | None,
    completed_at: datetime,
) -> None:
    event = {
        "event_id": str(uuid.uuid4()),      # idempotency key for consumers
        "event_type": COURSE_COMPLETED,
        "enrollment_id": str(enrollment_id),
        "student_id": str(student_id),
        "course_id": str(course_id),
        "started_at": started_at.isoformat() if started_at else None,
        "completed_at": completed_at.isoformat(),
        "occurred_at": datetime.now(timezone.utc).isoformat(),
    }
    publish_event(settings.KAFKA_PROGRESS_TOPIC, key=str(course_id), event=event)
