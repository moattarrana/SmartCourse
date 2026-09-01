"""Analytics service entrypoint. Starts the Kafka consumer in the background
and serves the analytics read API."""
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.routes import analytics
from app.consumer.analytics_consumer import start_consumer, stop_consumer
from app.core.config import settings
from app.core.db import processed_events

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(settings.SERVICE_NAME)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Ensure the idempotency ledger has its unique _id index (it does by default,
    # _id is always unique) and touch the connection early.
    processed_events()
    start_consumer()
    logger.info("%s started", settings.SERVICE_NAME)
    yield
    stop_consumer()


app = FastAPI(title="SmartCourse Analytics Service", version="0.1.0", lifespan=lifespan)
app.include_router(analytics.router)


@app.get("/health", tags=["meta"])
def health() -> dict:
    return {"status": "ok", "service": settings.SERVICE_NAME}
