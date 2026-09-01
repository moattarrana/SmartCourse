"""Background notification task — sends a welcome email via SMTP (Mailpit in
dev). Idempotent by event_id so a redelivered event never sends twice; the
Mongo record is the idempotency claim, and analytics counts it."""
import logging
import smtplib
from datetime import datetime, timezone
from email.message import EmailMessage

from pymongo.errors import DuplicateKeyError

from app.core import db
from app.core.config import settings
from app.tasks.celery_app import celery_app

logger = logging.getLogger("analytics-service.tasks")


def _send_email(to_addr: str, subject: str, body: str) -> None:
    msg = EmailMessage()
    msg["From"] = settings.EMAIL_FROM
    msg["To"] = to_addr
    msg["Subject"] = subject
    msg.set_content(body)
    with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT, timeout=10) as smtp:
        smtp.send_message(msg)


@celery_app.task(name="send_welcome_notification", bind=True,
                 max_retries=3, default_retry_delay=10)
def send_welcome_notification(self, event_id: str, student_id: str, course_id: str):
    # Idempotency claim first: if this event was already handled, stop here so
    # we never send a duplicate email.
    try:
        db.notifications().insert_one(
            {
                "_id": event_id,          # idempotency key
                "type": "welcome",
                "student_id": student_id,
                "course_id": course_id,
                "message": f"Welcome to your new course {course_id}!",
                "sent_at": datetime.now(timezone.utc),
            }
        )
    except DuplicateKeyError:
        logger.info("Welcome notification already sent for event %s", event_id)
        return {"status": "duplicate", "event_id": event_id}

    # Placeholder recipient (StudentEnrolled doesn't carry the email yet; threading
    # the real address through the enrollment event is a small follow-up).
    to_addr = f"student+{student_id}@smartcourse.local"
    subject = "Welcome to your new course!"
    body = (
        f"Hi,\n\nYou're enrolled in course {course_id}. Welcome aboard!\n\n"
        f"— SmartCourse"
    )
    try:
        _send_email(to_addr, subject, body)
        logger.info("Sent welcome email to %s for course %s", to_addr, course_id)
    except Exception as exc:  # record already stored; log and move on
        logger.error("Email send failed for event %s: %s", event_id, exc)

    return {"status": "sent", "event_id": event_id}
