"""Database engine + session management for the user service.

SQLAlchemy 2.0 style. A single Engine per process; a new Session per request,
handed out by the get_db() dependency and always closed afterwards.
"""
from collections.abc import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from app.core.config import settings

engine = create_engine(settings.DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


class Base(DeclarativeBase):
    """Declarative base all ORM models inherit from."""


def get_db() -> Generator[Session, None, None]:
    """FastAPI dependency: yields a Session and guarantees it is closed."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db() -> None:
    """Create tables from the ORM metadata.

    Fine for Week 1 because the schema is stable. Week 2 swaps this for Alembic
    migrations once the schema starts changing (enrollment, modules, lessons).
    Import models here so they are registered on Base.metadata before create_all.
    """
    from app.models import user  # noqa: F401

    Base.metadata.create_all(bind=engine)
