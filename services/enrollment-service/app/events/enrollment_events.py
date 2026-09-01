"""Builders + publisher for enrollment domain events."""
import uuid
from datetime import datetime, timezone

from app.core.config import settings
from app.events.producer import publish_event

STUDENT_ENROLLED = "StudentEnrolled"

# called when student is enrolled 
def publish_student_enrolled(
    enrollment_id: uuid.UUID, 
    student_id: uuid.UUID,
    course_id: uuid.UUID
) -> None:
    event = {
        "event_id": str(uuid.uuid4()),      # idempotency key for consumers
        "event_type": STUDENT_ENROLLED,
        "enrollment_id": str(enrollment_id),
        "student_id": str(student_id),
        "course_id": str(course_id),
        "occurred_at": datetime.now(timezone.utc).isoformat(),
    }
    # Key by course_id so all events for a course land on the same partition
    # (keeps per-course ordering once we scale partitions).
    publish_event(settings.KAFKA_ENROLLMENT_TOPIC, key=str(course_id), event=event)
