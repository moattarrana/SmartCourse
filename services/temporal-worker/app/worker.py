"""Worker entrypoint: `python -m app.worker`.

Runs two workers on one Temporal connection:
  - publishing worker on the course-publishing queue
  - enrollment worker on the enrollment-processing queue
Both run their (blocking) activities in a shared thread pool.
"""
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from temporalio.client import Client
from temporalio.worker import Worker

from app.activities.publishing_activities import (
    begin_publishing,
    emit_course_published,
    mark_publish_failed,
    mark_published,
    process_content,
    validate_course,
)
from app.activities.enrollment_activities import (
    emit_student_enrolled,
    enqueue_welcome_notification,
    init_enrollment_progress,
    record_enrollment,
    rollback_enrollment,
)
from app.config import (
    ENROLLMENT_TASK_QUEUE,
    TASK_QUEUE,
    TEMPORAL_HOST,
    TEMPORAL_NAMESPACE,
)
from app.workflow.enrollment_workflow import EnrollmentWorkflow
from app.workflow.publishing_workflow import CoursePublishingWorkflow

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("temporal-worker")


async def main() -> None:
    client = await Client.connect(TEMPORAL_HOST, namespace=TEMPORAL_NAMESPACE)
    logger.info(
        "Worker connected to %s; queues: %s, %s",
        TEMPORAL_HOST, TASK_QUEUE, ENROLLMENT_TASK_QUEUE,
    )
    with ThreadPoolExecutor(max_workers=10) as executor: #Running them in a thread pool
        # lets several blocking activities run at once (up to 10), keeping the worker responsive.
        publishing_worker = Worker(
            client,
            task_queue=TASK_QUEUE,
            workflows=[CoursePublishingWorkflow],
            activities=[
                begin_publishing,
                validate_course,
                process_content,
                mark_published,
                emit_course_published,
                mark_publish_failed,
            ],
            activity_executor=executor,
        )
        enrollment_worker = Worker(
            client,
            task_queue=ENROLLMENT_TASK_QUEUE,
            workflows=[EnrollmentWorkflow],
            activities=[
                record_enrollment,
                init_enrollment_progress,
                emit_student_enrolled,
                enqueue_welcome_notification,
                rollback_enrollment,
            ],
            activity_executor=executor,
        )
        await asyncio.gather(publishing_worker.run(), enrollment_worker.run())


if __name__ == "__main__":
    asyncio.run(main())
