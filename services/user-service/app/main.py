"""User service application entrypoint."""
import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from app.api.routes import auth, users
from app.core.config import settings
from app.core.database import engine, init_db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(settings.SERVICE_NAME)


def _wait_for_db(retries: int = 10, delay: float = 2.0) -> None:
    """Postgres may not be ready the instant the container starts."""
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


app = FastAPI(title="SmartCourse User Service", version="0.1.0", lifespan=lifespan)
app.include_router(auth.router)
app.include_router(users.router)


@app.get("/health", tags=["meta"])  # maps an HTTP GET request to the function beneath it
def health() -> dict:
    return {"status": "ok", "service": settings.SERVICE_NAME}
