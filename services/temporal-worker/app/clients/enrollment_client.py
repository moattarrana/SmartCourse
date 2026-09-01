"""Client for enrollment-service's internal API.

Owns *how* the worker talks to enrollment-service — base URL, auth header, HTTP
calls, and translating responses into outcomes/exceptions. The EnrollmentWorkflow
activities call this so the worker never touches enrollments_db directly.
Mirrors CourseServiceClient.
"""
import httpx
from temporalio.exceptions import ApplicationError

from app.config import ENROLLMENT_SERVICE_URL, INTERNAL_API_KEY


class EnrollmentServiceClient:
    def __init__(
        self,
        base_url: str = ENROLLMENT_SERVICE_URL,
        internal_key: str = INTERNAL_API_KEY,
        timeout: float = 10.0,
    ) -> None:
        self._base_url = base_url
        self._headers = {"X-Internal-Key": internal_key}
        self._timeout = timeout

    def _url(self, path: str) -> str: #A small helper that builds the full address for a given action. 
        #Give it "record" and it returns http://enrollment-service:8000/internal/enrollments/record.
        return f"{self._base_url}/internal/enrollments/{path}"

    def _post(self, path: str, payload: dict) -> dict:
        with httpx.Client(timeout=self._timeout) as client: #opens an HTTP client
            resp = client.post(self._url(path), headers=self._headers, json=payload) #response
        # A duplicate/full enrollment is a permanent condition -> non-retryable.
        if resp.status_code == 409:
            detail = resp.json().get("detail", "conflict")
            raise ApplicationError(detail, type="EnrollmentConflict", non_retryable=True)
        resp.raise_for_status()  # transient errors -> retryable
        return resp.json()

    def record_enrollment(self, enrollment_id: str, student_id: str, course_id: str) -> dict:
        return self._post(
            "record",
            {"enrollment_id": enrollment_id, "student_id": student_id, "course_id": course_id},
        )

    def init_progress(self, enrollment_id: str, student_id: str, course_id: str) -> dict:
        return self._post(
            "progress",
            {"enrollment_id": enrollment_id, "student_id": student_id, "course_id": course_id},
        )

    def emit_enrolled(self, enrollment_id: str, student_id: str, course_id: str) -> dict:
        return self._post(
            "emit-enrolled",
            {"enrollment_id": enrollment_id, "student_id": student_id, "course_id": course_id},
        )

    def rollback(self, enrollment_id: str, student_id: str, course_id: str) -> dict:
        return self._post(
            "rollback",
            {"enrollment_id": enrollment_id, "student_id": student_id, "course_id": course_id},
        )
