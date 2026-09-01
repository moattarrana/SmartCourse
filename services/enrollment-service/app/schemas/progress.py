"""Pydantic schemas for progress."""
import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from app.models.progress import ProgressStatus


class ProgressUpdate(BaseModel):
    percent: int = Field(ge=0, le=100)


class ProgressRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    enrollment_id: uuid.UUID
    student_id: uuid.UUID
    course_id: uuid.UUID
    status: ProgressStatus
    percent: int
    started_at: datetime | None
    completed_at: datetime | None
    created_at: datetime
    updated_at: datetime
