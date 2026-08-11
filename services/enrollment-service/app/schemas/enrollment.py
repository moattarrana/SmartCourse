"""Pydantic schemas for enrollments."""
import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict

from app.models.enrollment import EnrollmentStatus


class EnrollmentCreate(BaseModel):
    course_id: uuid.UUID


class EnrollmentRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    student_id: uuid.UUID
    course_id: uuid.UUID
    status: EnrollmentStatus
    enrolled_at: datetime
    created_at: datetime
