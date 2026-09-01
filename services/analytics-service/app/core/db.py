"""MongoDB client + collection handles for the analytics store."""
from pymongo import MongoClient

from app.core.config import settings

_client: MongoClient | None = None


def get_db():
    global _client
    if _client is None:
        _client = MongoClient(settings.MONGO_URL)
    return _client[settings.MONGO_DB]


# Collections:
#   processed_events   -> idempotency ledger for analytics (_id = event_id)
#   counters           -> { _id: "total_enrollments", value: N }
#   course_enrollments -> { _id: course_id, count: N }
#   notifications      -> sent notifications, idempotent by _id = event_id
def processed_events():
    return get_db()["processed_events"]


def counters():
    return get_db()["counters"]


def course_enrollments():
    return get_db()["course_enrollments"]


def notifications():
    return get_db()["notifications"]
