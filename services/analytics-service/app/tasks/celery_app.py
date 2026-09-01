"""Celery application: RabbitMQ broker, Redis result backend.

acks_late + prefetch=1 give a sane reliability/backpressure posture: a task is
re-delivered if a worker dies mid-run, and a worker pulls one task at a time
rather than hoarding the queue.
"""
from celery import Celery

from app.core.config import settings

celery_app = Celery(
    "analytics",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
    include=["app.tasks.notifications"],
)

celery_app.conf.task_acks_late = True
celery_app.conf.worker_prefetch_multiplier = 1
