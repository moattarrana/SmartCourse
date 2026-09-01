"""Client for course-service's internal API.


"""
import httpx
from temporalio.exceptions import ApplicationError

from app.config import COURSE_SERVICE_URL, INTERNAL_API_KEY


class CourseServiceClient:
    def __init__(
        self,
        base_url: str = COURSE_SERVICE_URL,
        internal_key: str = INTERNAL_API_KEY,
        timeout: float = 10.0,
    ) -> None:
        self._base_url = base_url
        self._headers = {"X-Internal-Key": internal_key}
        self._timeout = timeout

    def _internal(self, course_id: str) -> str:
        return f"{self._base_url}/courses/{course_id}/internal"

    def get_publish_check(self, course_id: str) -> dict:
        """Return {status, has_content} for the course, or raise if missing."""
        url = f"{self._internal(course_id)}/publish-check"
        with httpx.Client(timeout=self._timeout) as client:
            resp = client.get(url, headers=self._headers)
        if resp.status_code == 404:
            raise ApplicationError("Course not found", non_retryable=True)
        resp.raise_for_status()  # other errors -> retryable
        return resp.json()

    def set_status(self, course_id: str, status: str) -> None:
        """Apply a course status transition via course-service."""
        url = f"{self._internal(course_id)}/status"
        with httpx.Client(timeout=self._timeout) as client:
            resp = client.post(url, headers=self._headers, json={"status": status})
        if resp.status_code == 404:
            raise ApplicationError("Course not found", non_retryable=True)
        resp.raise_for_status()  # transient course-service errors -> retryable
