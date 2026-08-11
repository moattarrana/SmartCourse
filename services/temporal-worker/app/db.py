"""Minimal SQLAlchemy setup + ORM model for the worker.

The worker is the async arm of the course service and writes to the SAME
courses_db. We redeclare just the columns we touch, using the identical Enum
definition (name="course_status") so status reads/writes map exactly the way
course-service maps them — no name/value mismatch.
"""
import enum
import uuid

from sqlalchemy import Enum as SAEnum
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

from app.config import DATABASE_URL

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


class Base(DeclarativeBase):
    pass


class CourseStatus(str, enum.Enum):
    DRAFT = "draft"
    PUBLISHING = "publishing"
    PUBLISHED = "published"
    ARCHIVED = "archived"


class Course(Base):
    __tablename__ = "courses"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    status: Mapped[CourseStatus] = mapped_column(
        SAEnum(CourseStatus, name="course_status"), nullable=False
    )
