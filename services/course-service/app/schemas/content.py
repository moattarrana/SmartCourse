"""Pydantic schemas for modules and lessons."""
import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class LessonCreate(BaseModel):
    title: str = Field(min_length=1, max_length=255)
    content: str = Field(default="", max_length=100000)
    position: int = 0


class LessonUpdate(BaseModel):
    title: str | None = Field(default=None, min_length=1, max_length=255)
    content: str | None = Field(default=None, max_length=100000)
    position: int | None = None


class LessonRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    module_id: uuid.UUID
    title: str
    content: str
    position: int
    created_at: datetime
    updated_at: datetime


class ModuleCreate(BaseModel):
    title: str = Field(min_length=1, max_length=255)
    position: int = 0


class ModuleUpdate(BaseModel):
    title: str | None = Field(default=None, min_length=1, max_length=255)
    position: int | None = None


class ModuleRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    course_id: uuid.UUID
    title: str
    position: int
    created_at: datetime
    updated_at: datetime


class ModuleWithLessons(ModuleRead):
    lessons: list[LessonRead] = []
