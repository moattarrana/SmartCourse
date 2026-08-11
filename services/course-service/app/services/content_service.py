"""Business logic for modules and lessons."""
import uuid

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models.content import Lesson, Module
from app.schemas.content import (
    LessonCreate,
    LessonUpdate,
    ModuleCreate,
    ModuleUpdate,
)


class ModuleNotFoundError(Exception):
    pass


class LessonNotFoundError(Exception):
    pass


# ---- Modules ----
def create_module(db: Session, course_id: uuid.UUID, data: ModuleCreate) -> Module:
    module = Module(course_id=course_id, title=data.title, position=data.position)
    db.add(module)
    db.commit()
    db.refresh(module)
    return module


def list_modules(db: Session, course_id: uuid.UUID) -> list[Module]:
    stmt = (
        select(Module)
        .where(Module.course_id == course_id)
        .order_by(Module.position, Module.created_at)
    )
    return list(db.scalars(stmt).all())


def get_module(db: Session, course_id: uuid.UUID, module_id: uuid.UUID) -> Module:
    module = db.get(Module, module_id)
    if module is None or module.course_id != course_id:
        raise ModuleNotFoundError(str(module_id))
    return module


def update_module(
    db: Session, course_id: uuid.UUID, module_id: uuid.UUID, data: ModuleUpdate
) -> Module:
    module = get_module(db, course_id, module_id)
    if data.title is not None:
        module.title = data.title
    if data.position is not None:
        module.position = data.position
    db.commit()
    db.refresh(module)
    return module


def delete_module(db: Session, course_id: uuid.UUID, module_id: uuid.UUID) -> None:
    module = get_module(db, course_id, module_id)
    db.delete(module)
    db.commit()


# ---- Lessons ----
def create_lesson(
    db: Session, course_id: uuid.UUID, module_id: uuid.UUID, data: LessonCreate
) -> Lesson:
    get_module(db, course_id, module_id)  # validates module belongs to course
    lesson = Lesson(
        module_id=module_id,
        title=data.title,
        content=data.content,
        position=data.position,
    )
    db.add(lesson)
    db.commit()
    db.refresh(lesson)
    return lesson


def list_lessons(
    db: Session, course_id: uuid.UUID, module_id: uuid.UUID
) -> list[Lesson]:
    get_module(db, course_id, module_id)
    stmt = (
        select(Lesson)
        .where(Lesson.module_id == module_id)
        .order_by(Lesson.position, Lesson.created_at)
    )
    return list(db.scalars(stmt).all())


def get_lesson(
    db: Session, course_id: uuid.UUID, module_id: uuid.UUID, lesson_id: uuid.UUID
) -> Lesson:
    get_module(db, course_id, module_id)
    lesson = db.get(Lesson, lesson_id)
    if lesson is None or lesson.module_id != module_id:
        raise LessonNotFoundError(str(lesson_id))
    return lesson


def update_lesson(
    db: Session,
    course_id: uuid.UUID,
    module_id: uuid.UUID,
    lesson_id: uuid.UUID,
    data: LessonUpdate,
) -> Lesson:
    lesson = get_lesson(db, course_id, module_id, lesson_id)
    if data.title is not None:
        lesson.title = data.title
    if data.content is not None:
        lesson.content = data.content
    if data.position is not None:
        lesson.position = data.position
    db.commit()
    db.refresh(lesson)
    return lesson


def delete_lesson(
    db: Session, course_id: uuid.UUID, module_id: uuid.UUID, lesson_id: uuid.UUID
) -> None:
    lesson = get_lesson(db, course_id, module_id, lesson_id)
    db.delete(lesson)
    db.commit()


def course_has_content(db: Session, course_id: uuid.UUID) -> bool:
    """True if the course has at least one lesson (publishable)."""
    stmt = (
        select(func.count(Lesson.id))
        .join(Module, Lesson.module_id == Module.id)
        .where(Module.course_id == course_id)
    )
    return (db.scalar(stmt) or 0) > 0
