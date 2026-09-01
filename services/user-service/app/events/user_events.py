"""Builders + publisher for user domain events."""
import uuid
from datetime import datetime, timezone

from app.core.config import settings
from app.events.producer import publish_event

USER_REGISTERED = "UserRegistered"


def publish_user_registered(user_id: uuid.UUID, role: str) -> None:
    event = {
        "event_id": str(uuid.uuid4()),   # idempotency key for consumers
        "event_type": USER_REGISTERED,
        "user_id": str(user_id),
        "role": role,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
    }
    publish_event(settings.KAFKA_USER_TOPIC, key=str(user_id), event=event)
