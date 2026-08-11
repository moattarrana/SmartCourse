"""Cross-service check: ask the course service whether a course is enrollable.

enrollments_db has no copy of course state, so we call the course service,
forwarding the caller's JWT for authorization.
"""
import uuid

import httpx

from app.core.config import settings


class CourseNotFoundError(Exception):
    pass


class CourseNotPublishedError(Exception):
    pass


async def assert_course_enrollable(token: str, course_id: uuid.UUID) -> None:
    url = f"{settings.COURSE_SERVICE_URL}/courses/{course_id}"
    headers = {"Authorization": f"Bearer {token}"}
    async with httpx.AsyncClient(timeout=settings.REQUEST_TIMEOUT_SECONDS) as client:
        resp = await client.get(url, headers=headers)
    if resp.status_code == 404:
        raise CourseNotFoundError(str(course_id))
    resp.raise_for_status()
    if resp.json().get("status") != "published":
        raise CourseNotPublishedError(str(course_id))
