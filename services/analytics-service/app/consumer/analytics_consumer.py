"""Background Kafka consumer that feeds the analytics store.

Subscribes to the user, enrollment, course, and progress event topics and updates
metrics. Notifications are no longer triggered here: the enrollment Temporal
workflow enqueues the welcome-notification task directly. Analytics is metrics-only.
"""
import json
import logging
import threading

from confluent_kafka import Consumer

from app.core.config import settings
from app.services import analytics_service

logger = logging.getLogger(settings.SERVICE_NAME)

_stop = threading.Event()
_thread: threading.Thread | None = None


def _handle(event: dict) -> None:
    event_type = event.get("event_type")
    if event_type == "UserRegistered":
        analytics_service.record_user_registered(event)
    elif event_type == "StudentEnrolled":
        analytics_service.record_student_enrolled(event)
    elif event_type == "CoursePublished":
        analytics_service.record_course_published(event)
    elif event_type == "CourseCompleted":
        analytics_service.record_course_completed(event)
    else:
        logger.debug("Ignoring event type %s", event_type)


def _run() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": settings.KAFKA_GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )
    topics = [
        settings.KAFKA_USER_TOPIC,
        settings.KAFKA_ENROLLMENT_TOPIC,
        settings.KAFKA_COURSE_TOPIC,
        settings.KAFKA_PROGRESS_TOPIC,
    ]
    consumer.subscribe(topics)
    logger.info("Analytics consumer subscribed to %s", topics)
    try:
        while not _stop.is_set():
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("Consumer error: %s", msg.error())
                continue
            try:
                event = json.loads(msg.value().decode("utf-8"))
                _handle(event)
            except Exception as exc:
                logger.exception("Failed to process message: %s", exc)
                try:
                    analytics_service.increment_failed_events()
                except Exception:
                    pass
    finally:
        consumer.close()
        logger.info("Analytics consumer stopped")


def start_consumer() -> None:
    global _thread
    _stop.clear()
    _thread = threading.Thread(target=_run, name="analytics-consumer", daemon=True)
    _thread.start()


def stop_consumer() -> None:
    _stop.set()
    if _thread is not None:
        _thread.join(timeout=5)
