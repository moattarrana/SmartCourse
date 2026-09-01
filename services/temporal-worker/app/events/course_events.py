"""Course-domain events published to Kafka by the publishing workflow."""
import uuid
from datetime import datetime, timezone

from app.config import KAFKA_COURSE_TOPIC
from app.events.kafka import publish


def send_course_published(course_id: str) -> None:
    """Announce that a course went live. 'send' = publish to Kafka;
    'course_published' is the event name (avoids the old double 'publish')."""
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "CoursePublished",
        "course_id": str(course_id),
        "occurred_at": datetime.now(timezone.utc).isoformat(),
    }
    publish(KAFKA_COURSE_TOPIC, key=str(course_id), event=event)
