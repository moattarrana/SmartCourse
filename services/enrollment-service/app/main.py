"""Enrollment service application entrypoint."""
import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from app.api.routes import enrollments, internal
from app.core.config import settings
from app.core.database import engine, init_db
from app.core.logging_setup import setup_logging
from app.core.telemetry import setup_telemetry

setup_logging(settings.SERVICE_NAME)
logger = logging.getLogger(settings.SERVICE_NAME)


def _wait_for_db(retries: int = 10, delay: float = 2.0) -> None:
    for attempt in range(1, retries + 1):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return
        except OperationalError:
            logger.warning("DB not ready (attempt %s/%s), retrying...", attempt, retries)
            time.sleep(delay)
    raise RuntimeError("Database did not become available in time")


@asynccontextmanager
async def lifespan(app: FastAPI):
    _wait_for_db()
    init_db()
    logger.info("%s started", settings.SERVICE_NAME)
    yield


app = FastAPI(title="SmartCourse Enrollment Service", version="0.3.0", lifespan=lifespan)
app.include_router(enrollments.router)
app.include_router(internal.router)

setup_telemetry(app, settings.SERVICE_NAME, settings.OTEL_EXPORTER_OTLP_ENDPOINT)
Instrumentator().instrument(app).expose(app)   # prometheu`s metrics endpoint at /metrics`


@app.get("/health", tags=["meta"])
def health() -> dict:
    return {"status": "ok", "service": settings.SERVICE_NAME}
