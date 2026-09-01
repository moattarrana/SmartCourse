"""Celery client used by the enrollment workflow to enqueue the welcome
notification task.

Kept separate from the Kafka event producers on purpose: this is a task-queue
(RabbitMQ) *enqueue*, not an event *publish*. The worker only produces the task
by name; the celery-worker in the analytics image runs it. No result backend is
configured because the worker never waits for or reads the task's return value.
"""
import logging

from celery import Celery

from app.config import CELERY_BROKER_URL, NOTIFICATION_TASK_NAME

logger = logging.getLogger("temporal-worker.notifier")

_celery: Celery | None = None


def _get_celery() -> Celery:
    global _celery
    if _celery is None:
        _celery = Celery("temporal-worker", broker=CELERY_BROKER_URL)
    return _celery


def enqueue_welcome_notification(
    enrollment_id: str, student_id: str, course_id: str
) -> None:
    app = _get_celery()
    app.send_task(
        NOTIFICATION_TASK_NAME,
        args=[str(enrollment_id), str(student_id), str(course_id)],
    )
    logger.info("Enqueued welcome notification enrollment=%s", enrollment_id)
