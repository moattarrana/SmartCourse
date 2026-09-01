"""Builders + publisher for course domain events."""
import uuid
from datetime import datetime, timezone

from app.core.config import settings
from app.events.producer import publish_event

COURSE_PUBLISHED = "CoursePublished"


def publish_course_published(course_id: str) -> None:
    event = {
        "event_id": str(uuid.uuid4()),      # idempotency key for consumers
        "event_type": COURSE_PUBLISHED,
        "course_id": str(course_id),
        "occurred_at": datetime.now(timezone.utc).isoformat(),
    }
    publish_event(settings.KAFKA_COURSE_TOPIC, key=str(course_id), event=event)
