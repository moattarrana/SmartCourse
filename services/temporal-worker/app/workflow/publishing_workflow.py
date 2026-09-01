"""CoursePublishingWorkflow — begin, validate, process, publish, announce;
rollback handled by a RollbackManager (Saga pattern).

The draft->publishing flip is the workflow's first activity (begin_publishing),
so the workflow owns the entire status transition. If the workflow never starts
(e.g. Temporal is down), the course stays draft and nothing can strand."""
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

from app.workflow.saga import RollbackManager

_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    maximum_interval=timedelta(seconds=10),
    maximum_attempts=3,
)


@workflow.defn(name="CoursePublishingWorkflow")
class CoursePublishingWorkflow:
    @workflow.run
    async def run(self, course_id: str) -> str:
        workflow.logger.info("Publishing workflow started for course %s", course_id)
        saga = RollbackManager()
        try:
            # Step 0: flip draft -> publishing (idempotent), owned by the workflow.
            #activities by string so the workflow module 
            #never imports the activity code (and its Kafka/HTTP/client dependencies),
            # which keeps the workflow's import path guaranteed deterministic 
            # for Temporal's replay — no sandbox pass-through needed.
            await workflow.execute_activity(
                "begin_publishing", course_id,
                start_to_close_timeout=timedelta(seconds=30), retry_policy=_RETRY,
            )
            saga.add("mark_publish_failed", [course_id])  # register undo AFTER the flip

            workflow.logger.info("Step 1/3: validating course %s", course_id)
            await workflow.execute_activity(
                "validate_course", course_id,
                start_to_close_timeout=timedelta(seconds=30), retry_policy=_RETRY,
            )

            workflow.logger.info("Step 2/3: processing content for course %s", course_id)
            await workflow.execute_activity(
                "process_content", course_id,
                start_to_close_timeout=timedelta(minutes=5), retry_policy=_RETRY,
            )

            workflow.logger.info("Step 3/3: marking course %s published", course_id)
            await workflow.execute_activity(
                "mark_published", course_id,
                start_to_close_timeout=timedelta(seconds=30), retry_policy=_RETRY,
            )
        except ActivityError:
            workflow.logger.warning(
                "Publishing failed for course %s; rolling back", course_id
            )
            await saga.rollback()
            raise

        # Best-effort announcement (course is already live; a failed emit must not undo it).
        try:
            await workflow.execute_activity(
                "emit_course_published", course_id,
                start_to_close_timeout=timedelta(seconds=30), retry_policy=_RETRY,
            )
        except ActivityError as exc:
            workflow.logger.warning(
                "CoursePublished emit failed for %s (%s); course remains published",
                course_id, exc,
            )

        workflow.logger.info("Publishing workflow completed for course %s", course_id)
        return "published"
