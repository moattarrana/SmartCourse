"""Activities for the EnrollmentWorkflow.

Record and progress steps delegate to EnrollmentServiceClient (the worker never
touches enrollments_db). The event emit and the notification enqueue are now done
by the worker directly: publishing to Kafka and enqueuing to RabbitMQ are
shared-infrastructure access, not database access, so the no-database rule holds."""
from temporalio import activity

from app.events import enrollment_events
from app.notifications import notifier
from app.clients.enrollment_client import EnrollmentServiceClient

_client = EnrollmentServiceClient()


@activity.defn(name="record_enrollment")
def record_enrollment(enrollment_id: str, student_id: str, course_id: str) -> None:
    activity.logger.info("record_enrollment START enrollment=%s", enrollment_id)
    _client.record_enrollment(enrollment_id, student_id, course_id) #calls the client,
    #which makes an HTTP POST to enrollment-service's internal /record endpoint, which creates the database row.
    activity.logger.info("record_enrollment OK enrollment=%s", enrollment_id)


@activity.defn(name="init_enrollment_progress")
def init_enrollment_progress(enrollment_id: str, student_id: str, course_id: str) -> None:
    activity.logger.info("init_enrollment_progress START enrollment=%s", enrollment_id)
    _client.init_progress(enrollment_id, student_id, course_id)
    activity.logger.info("init_enrollment_progress OK enrollment=%s", enrollment_id)


@activity.defn(name="emit_student_enrolled")
def emit_student_enrolled(enrollment_id: str, student_id: str, course_id: str) -> None:
    """Publish StudentEnrolled to Kafka directly (was previously emitted by
    enrollment-service via an internal endpoint)."""
    activity.logger.info("emit_student_enrolled START enrollment=%s", enrollment_id)
    enrollment_events.send_student_enrolled(enrollment_id, student_id, course_id) #hands it to Kafka (via kafka.py's publish
    activity.logger.info("emit_student_enrolled OK enrollment=%s", enrollment_id)


@activity.defn(name="enqueue_welcome_notification")
def enqueue_welcome_notification(enrollment_id: str, student_id: str, course_id: str) -> None:
    """Enqueue the welcome-notification task to RabbitMQ directly (was previously
    triggered by analytics-service). Idempotency key is the enrollment_id, so a
    retried step never produces a second email."""
    activity.logger.info("enqueue_welcome_notification START enrollment=%s", enrollment_id)
    notifier.enqueue_welcome_notification(enrollment_id, student_id, course_id) #drops the email task onto RabbitMQ (via Celery).
    activity.logger.info("enqueue_welcome_notification OK enrollment=%s", enrollment_id)


@activity.defn(name="rollback_enrollment")
def rollback_enrollment(enrollment_id: str, student_id: str, course_id: str) -> None:
    """Compensation: remove the enrollment row if the workflow failed."""
    activity.logger.info("rollback_enrollment START enrollment=%s", enrollment_id)
    _client.rollback(enrollment_id, student_id, course_id) #TTP to enrollment-service's internal /rollback endpoint → deletes the enrollment row
    activity.logger.info("rollback_enrollment OK enrollment=%s rolled back", enrollment_id)