"""Enrollment workflow: orchestrates the core enrollment steps durably.
Steps: record enrollment, init progress, publish StudentEnrolled to Kafka
(worker), then enqueue the welcome notification to RabbitMQ (worker). The first
three are compensated on failure via a RollbackManager (Saga). The notification
is best-effort: a failure there does not undo the enrollment. Imports only
stdlib + temporalio + the saga helper so the workflow stays deterministic;
activities are referenced by string name."""
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


@workflow.defn(name="EnrollmentWorkflow")
class EnrollmentWorkflow:
    @workflow.run
    async def run(self, enrollment_id: str, student_id: str, course_id: str) -> str:
        args = [enrollment_id, student_id, course_id]
        workflow.logger.info(
            "Enrollment workflow started enrollment=%s student=%s course=%s",
            enrollment_id, student_id, course_id,
        )
        saga = RollbackManager() #Creates a fresh rollback manager for this run.
        #Right now its undo-list is empty; we'll register undos into it as steps succeed.
        try:
            workflow.logger.info("Step 1/3: recording enrollment %s", enrollment_id)
            await workflow.execute_activity(
                "record_enrollment", args=args,  #record_enrollment — creates a database row
                #activities by string so the workflow module 
                #never imports the activity code (and its Kafka/HTTP/client dependencies),
                # which keeps the workflow's import path guaranteed deterministic 
                # for Temporal's replay — no sandbox pass-through needed.
                start_to_close_timeout=timedelta(seconds=30), retry_policy=_RETRY,
            )
            saga.add("rollback_enrollment", args)  # undo the row if a later step fails

            workflow.logger.info("Step 2/3: initializing progress %s", enrollment_id)
            await workflow.execute_activity(
                "init_enrollment_progress", args=args, #creates the progress row, which is a child of the enrollment. 
                #sDeleting the enrollment already removes it
                start_to_close_timeout=timedelta(seconds=30), retry_policy=_RETRY,
            )
            workflow.logger.info("Step 3/3: publishing StudentEnrolled %s", enrollment_id)
            await workflow.execute_activity(
                "emit_student_enrolled", args=args, #emit_student_enrolled — publishes an event to Kafka.
                start_to_close_timeout=timedelta(seconds=30), retry_policy=_RETRY,
            )
        except ActivityError as exc:
            workflow.logger.warning(
                "Enrollment failed enrollment=%s (%s); running compensation",
                enrollment_id, exc,
            )
            await saga.rollback() #run the undos. The saga runs everything registered, in reverse order.
            workflow.logger.info("Compensation done enrollment=%s", enrollment_id)
            raise
        # Best-effort notification: the student is already enrolled, so a failure
        # to enqueue the welcome email must NOT undo the enrollment.
        try:
            workflow.logger.info("Enqueuing welcome notification %s", enrollment_id)
            await workflow.execute_activity(
                "enqueue_welcome_notification", args=args, #(drops the email task on RabbitMQ)
                start_to_close_timeout=timedelta(seconds=30), retry_policy=_RETRY,
            )
        except ActivityError as exc:
            workflow.logger.warning(
                "Notification enqueue failed enrollment=%s (%s); enrollment still stands",
                enrollment_id, exc,
            )
        workflow.logger.info("Enrollment workflow completed enrollment=%s", enrollment_id)
        return "enrolled"
