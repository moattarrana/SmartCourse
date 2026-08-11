"""Worker entrypoint: `python -m app.worker`."""
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from temporalio.client import Client
from temporalio.worker import Worker

from app.activities import (
    mark_publish_failed,
    mark_published,
    process_content,
    validate_course,
)
from app.config import TASK_QUEUE, TEMPORAL_HOST, TEMPORAL_NAMESPACE
from app.workflow import CoursePublishingWorkflow

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("temporal-worker")


async def main() -> None:
    client = await Client.connect(TEMPORAL_HOST, namespace=TEMPORAL_NAMESPACE)
    logger.info("Worker connected to %s, task queue %s", TEMPORAL_HOST, TASK_QUEUE)
    # Sync (blocking DB) activities run in a thread pool; the workflow stays async.
    with ThreadPoolExecutor(max_workers=10) as executor:
        worker = Worker(
            client,
            task_queue=TASK_QUEUE,
            workflows=[CoursePublishingWorkflow],
            activities=[
                validate_course,
                process_content,
                mark_published,
                mark_publish_failed,
            ],
            activity_executor=executor,
        )
        await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
