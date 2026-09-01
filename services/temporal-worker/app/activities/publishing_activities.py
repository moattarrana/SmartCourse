"""Activities: the side-effecting steps of the publishing workflow.

Each activity is a thin wrapper that expresses what step happens and delegates
how to talk to course-service to CourseServiceClient. The CoursePublished event is
now published by the worker directly (publishing to Kafka is shared-infrastructure
access, not database access). Registered by name so the workflow references them as
strings and stays import-light and deterministic."""
import time

from temporalio import activity
from temporalio.exceptions import ApplicationError

from app.events import course_events
from app.clients.course_client import CourseServiceClient

_client = CourseServiceClient()


@activity.defn(name="begin_publishing")
def begin_publishing(course_id: str) -> None:
    """Flip the course draft -> publishing. This is the workflow's first step, so
    the workflow (not the endpoint) owns the entire status transition.

    Idempotent: if the course is already publishing (e.g. this activity is retried
    after a lost ack), it's a no-op, so a retry never spuriously fails."""
    activity.logger.info("begin_publishing START course=%s", course_id)
    data = _client.get_publish_check(course_id) #before changing anything, ask course-service for the current status
    if data["status"] == "publishing":
        activity.logger.info(
            "begin_publishing course=%s already publishing (no-op)", course_id
        )
        return
    if data["status"] != "draft":
        raise ApplicationError(
            f"Course is {data['status']}, cannot begin publishing", non_retryable=True
        )
    _client.set_status(course_id, "publishing")
    activity.logger.info("begin_publishing OK course=%s", course_id)


@activity.defn(name="validate_course")
def validate_course(course_id: str) -> None:
    activity.logger.info("validate_course START course=%s", course_id)
    data = _client.get_publish_check(course_id)
    if data["status"] != "publishing":
        activity.logger.warning(
            "validate_course FAIL course=%s status=%s", course_id, data["status"]
        )
        raise ApplicationError(
            f"Course is {data['status']}, expected publishing", non_retryable=True
        )
    if not data["has_content"]:
        activity.logger.warning("validate_course FAIL course=%s no content", course_id)
        raise ApplicationError("Course has no lessons", non_retryable=True)
    activity.logger.info("validate_course OK course=%s", course_id)


@activity.defn(name="process_content")
def process_content(course_id: str) -> None:
    """Process content before go-live. Placeholder step (simulated work) so
    retries and timeouts are demonstrable."""
    activity.logger.info("process_content START course=%s", course_id)
    time.sleep(2)
    activity.logger.info("process_content DONE course=%s", course_id)


@activity.defn(name="mark_published")
def mark_published(course_id: str) -> None:
    activity.logger.info("mark_published START course=%s", course_id)
    _client.set_status(course_id, "published")
    activity.logger.info("mark_published OK course=%s", course_id)


@activity.defn(name="emit_course_published")
def emit_course_published(course_id: str) -> None:
    """Publish CoursePublished to Kafka directly (was previously emitted by
    course-service when its status flipped to published)."""
    activity.logger.info("emit_course_published START course=%s", course_id)
    course_events.send_course_published(course_id)
    activity.logger.info("emit_course_published OK course=%s", course_id)


@activity.defn(name="mark_publish_failed")
def mark_publish_failed(course_id: str) -> None:
    """Compensation: return the course to DRAFT via course-service."""
    activity.logger.info("mark_publish_failed START course=%s", course_id)
    _client.set_status(course_id, "draft")
    activity.logger.info("mark_publish_failed OK course=%s rolled back to draft", course_id)
