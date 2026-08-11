"""Course publishing workflow. Imports only stdlib + temporalio so it stays
deterministic; activities are referenced by string name."""
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    maximum_interval=timedelta(seconds=10),
    maximum_attempts=3,
)


@workflow.defn(name="CoursePublishingWorkflow")
class CoursePublishingWorkflow:
    @workflow.run
    async def run(self, course_id: str) -> str:
        try:
            await workflow.execute_activity(
                "validate_course", course_id,
                start_to_close_timeout=timedelta(seconds=30), retry_policy=_RETRY,
            )
            await workflow.execute_activity(
                "process_content", course_id,
                start_to_close_timeout=timedelta(minutes=5), retry_policy=_RETRY,
            )
            await workflow.execute_activity(
                "mark_published", course_id,
                start_to_close_timeout=timedelta(seconds=30), retry_policy=_RETRY,
            )
            return "published"
        except ActivityError:
            await workflow.execute_activity(
                "mark_publish_failed", course_id,
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=RetryPolicy(maximum_attempts=5),
            )
            raise
