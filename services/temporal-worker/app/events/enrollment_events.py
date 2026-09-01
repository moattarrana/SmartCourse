"""Enrollment-domain events published to Kafka by the enrollment workflow."""
import uuid
from datetime import datetime, timezone

from app.config import KAFKA_ENROLLMENT_TOPIC
from app.events.kafka import publish


def send_student_enrolled(enrollment_id: str, student_id: str, course_id: str) -> None:
    """Announce that a student enrolled. 'send' = publish to Kafka."""
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "StudentEnrolled",
        "enrollment_id": str(enrollment_id),
        "student_id": str(student_id),
        "course_id": str(course_id),
        "occurred_at": datetime.now(timezone.utc).isoformat(),
    }
    publish(KAFKA_ENROLLMENT_TOPIC, key=str(course_id), event=event)
