"""Course request/response schemas."""
import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from app.models.course import CourseStatus


class CourseCreate(BaseModel):
    title: str = Field(min_length=1, max_length=255)
    description: str = Field(default="", max_length=10000)


class CourseUpdate(BaseModel):
    title: str | None = Field(default=None, min_length=1, max_length=255)
    description: str | None = Field(default=None, max_length=10000)
    status: CourseStatus | None = None


class CourseRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    title: str
    description: str
    instructor_id: uuid.UUID
    status: CourseStatus
    created_at: datetime
    updated_at: datetime
