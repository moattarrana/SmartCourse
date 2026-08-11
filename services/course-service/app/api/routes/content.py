"""Module + lesson endpoints, nested under a course.

Reads are open to any authenticated user; writes require the instructor role
AND ownership of the parent course. Paths sit under /courses/{id} so the
gateway's existing /api/courses rule forwards them here.
"""
import uuid

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.api.deps import CurrentUser, get_current_user, require_instructor
from app.core.database import get_db
from app.schemas.content import (
    LessonCreate,
    LessonRead,
    LessonUpdate,
    ModuleCreate,
    ModuleRead,
    ModuleUpdate,
    ModuleWithLessons,
)
from app.services import content_service, course_service

router = APIRouter(prefix="/courses/{course_id}", tags=["content"])


def _get_course_or_404(db: Session, course_id: uuid.UUID):
    try:
        return course_service.get_course(db, course_id)
    except course_service.CourseNotFoundError:
        raise HTTPException(status_code=404, detail="Course not found")


def _owned_course(db: Session, course_id: uuid.UUID, current: CurrentUser):
    course = _get_course_or_404(db, course_id)
    if course.instructor_id != current.id:
        raise HTTPException(status_code=403, detail="You do not own this course")
    return course


# ---- Modules ----
@router.post("/modules", response_model=ModuleRead, status_code=201)
def create_module(
    course_id: uuid.UUID,
    data: ModuleCreate,
    current: CurrentUser = Depends(require_instructor),
    db: Session = Depends(get_db),
) -> ModuleRead:
    _owned_course(db, course_id, current)
    return ModuleRead.model_validate(content_service.create_module(db, course_id, data))


@router.get("/modules", response_model=list[ModuleRead])
def list_modules(
    course_id: uuid.UUID,
    _: CurrentUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> list[ModuleRead]:
    _get_course_or_404(db, course_id)
    return [ModuleRead.model_validate(m) for m in content_service.list_modules(db, course_id)]


@router.get("/modules/{module_id}", response_model=ModuleWithLessons)
def get_module(
    course_id: uuid.UUID,
    module_id: uuid.UUID,
    _: CurrentUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> ModuleWithLessons:
    _get_course_or_404(db, course_id)
    try:
        return ModuleWithLessons.model_validate(
            content_service.get_module(db, course_id, module_id)
        )
    except content_service.ModuleNotFoundError:
        raise HTTPException(status_code=404, detail="Module not found")


@router.patch("/modules/{module_id}", response_model=ModuleRead)
def update_module(
    course_id: uuid.UUID,
    module_id: uuid.UUID,
    data: ModuleUpdate,
    current: CurrentUser = Depends(require_instructor),
    db: Session = Depends(get_db),
) -> ModuleRead:
    _owned_course(db, course_id, current)
    try:
        return ModuleRead.model_validate(
            content_service.update_module(db, course_id, module_id, data)
        )
    except content_service.ModuleNotFoundError:
        raise HTTPException(status_code=404, detail="Module not found")


@router.delete("/modules/{module_id}", status_code=204)
def delete_module(
    course_id: uuid.UUID,
    module_id: uuid.UUID,
    current: CurrentUser = Depends(require_instructor),
    db: Session = Depends(get_db),
) -> None:
    _owned_course(db, course_id, current)
    try:
        content_service.delete_module(db, course_id, module_id)
    except content_service.ModuleNotFoundError:
        raise HTTPException(status_code=404, detail="Module not found")


# ---- Lessons ----
@router.post("/modules/{module_id}/lessons", response_model=LessonRead, status_code=201)
def create_lesson(
    course_id: uuid.UUID,
    module_id: uuid.UUID,
    data: LessonCreate,
    current: CurrentUser = Depends(require_instructor),
    db: Session = Depends(get_db),
) -> LessonRead:
    _owned_course(db, course_id, current)
    try:
        return LessonRead.model_validate(
            content_service.create_lesson(db, course_id, module_id, data)
        )
    except content_service.ModuleNotFoundError:
        raise HTTPException(status_code=404, detail="Module not found")


@router.get("/modules/{module_id}/lessons", response_model=list[LessonRead])
def list_lessons(
    course_id: uuid.UUID,
    module_id: uuid.UUID,
    _: CurrentUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> list[LessonRead]:
    _get_course_or_404(db, course_id)
    try:
        return [
            LessonRead.model_validate(le)
            for le in content_service.list_lessons(db, course_id, module_id)
        ]
    except content_service.ModuleNotFoundError:
        raise HTTPException(status_code=404, detail="Module not found")


@router.get("/modules/{module_id}/lessons/{lesson_id}", response_model=LessonRead)
def get_lesson(
    course_id: uuid.UUID,
    module_id: uuid.UUID,
    lesson_id: uuid.UUID,
    _: CurrentUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> LessonRead:
    _get_course_or_404(db, course_id)
    try:
        return LessonRead.model_validate(
            content_service.get_lesson(db, course_id, module_id, lesson_id)
        )
    except (content_service.ModuleNotFoundError, content_service.LessonNotFoundError):
        raise HTTPException(status_code=404, detail="Lesson not found")


@router.patch("/modules/{module_id}/lessons/{lesson_id}", response_model=LessonRead)
def update_lesson(
    course_id: uuid.UUID,
    module_id: uuid.UUID,
    lesson_id: uuid.UUID,
    data: LessonUpdate,
    current: CurrentUser = Depends(require_instructor),
    db: Session = Depends(get_db),
) -> LessonRead:
    _owned_course(db, course_id, current)
    try:
        return LessonRead.model_validate(
            content_service.update_lesson(db, course_id, module_id, lesson_id, data)
        )
    except (content_service.ModuleNotFoundError, content_service.LessonNotFoundError):
        raise HTTPException(status_code=404, detail="Lesson not found")


@router.delete("/modules/{module_id}/lessons/{lesson_id}", status_code=204)
def delete_lesson(
    course_id: uuid.UUID,
    module_id: uuid.UUID,
    lesson_id: uuid.UUID,
    current: CurrentUser = Depends(require_instructor),
    db: Session = Depends(get_db),
) -> None:
    _owned_course(db, course_id, current)
    try:
        content_service.delete_lesson(db, course_id, module_id, lesson_id)
    except (content_service.ModuleNotFoundError, content_service.LessonNotFoundError):
        raise HTTPException(status_code=404, detail="Lesson not found")
