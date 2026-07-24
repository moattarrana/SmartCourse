# SmartCourse — Product Requirements Document (Part A)

Version 0.1 · Scope: Part A backend (Weeks 1–3). This PRD covers the full Part A
plan; the **Week 1** slice that is implemented today is marked throughout.

## 1. Overview

EduCorp's SmartCourse needs a backend that stays reliable and consistent as
learners and instructors scale into the tens of thousands. Part A delivers that
foundation: course management, a durable publishing workflow, high-volume
enrollment, and analytics — without any GenAI. It solves slow/manual publishing,
data inconsistency across course/enrollment/analytics, high latency under load,
unreliable background processing, and weak observability.

## 2. Key use-cases

- UC-1 — Register & authenticate. A person signs up as student or instructor, then logs in to receive a token. **(Week 1)**
- UC-2 — Manage courses. An instructor creates and edits courses; anyone can
  browse them. **(Week 1)**
- UC-3 — Role-based access. Only instructors create courses; only the owning
  instructor edits or deletes them. **(Week 1)**
- UC-4 — Publish a course. Publishing breaks content into modules/lessons/chunks
  and marks the course "ready" only after all processing succeeds, with no
  corruption on partial failure. (Week 2 — Temporal)
- UC-5 — Enroll in a course. Enrollment records the student, initializes progress,
  updates analytics, and triggers a welcome notification — idempotently and
  recoverably. (Week 2–3)
- UC-6 — Observe & analyze. Operators see platform metrics and can diagnose
  failures in publishing, enrollment, and background tasks. (Week 3)

## 3. Functional requirements

| ID   | Requirement                                                        | Milestone |
| ---- | ------------------------------------------------------------------ | --------- |
| FR-1 | Create/read/update users with roles student/instructor              | Week 1    |
| FR-2 | Authenticate users and issue verifiable access tokens              | Week 1    |
| FR-3 | Create/read/update/delete courses with a lifecycle status          | Week 1    |
| FR-4 | Enforce role- and ownership-based authorization on all writes      | Week 1    |
| FR-5 | Enrollment with duplicate handling, limits/prerequisites, history  | Week 2    |
| FR-6 | Course publishing workflow with modules/lessons/chunks             | Week 2    |
| FR-7 | Event-driven fan-out (analytics, notifications, indexing)          | Week 3    |
| FR-8 | Analytics metrics (see §6 of platform brief)                       | Week 3    |

## 4. Non-functional requirements

| ID    | Requirement                                                             |
| ----- | ----------------------------------------------------------------------- |
| NFR-1 | Consistency: each service is the single source of truth for its data.   |
| NFR-2 | Scalability: services scale independently; stateless request handling.  |
| NFR-3 | Reliability: partial failures must not corrupt state; workflows recover. |
| NFR-4 | Idempotency: repeated enrollment/analytics events do not double-apply.  |
| NFR-5 | Observability: key flows are logged, traceable, and measurable.         |
| NFR-6 | Security: passwords hashed; access gated by signed tokens and roles.    |
| NFR-7 | Portability: full stack runs locally via a single Docker Compose command.|

Week 1 addresses NFR-1, NFR-2 (structurally), NFR-6, and NFR-7. NFR-3/4/5 are
designed for now (status enum, UUIDs, service seams) and delivered in Weeks 2–3.

## 5. Milestones / timeline

| Week | Focus                          | Exit criteria                                        |
| ---- | ------------------------------ | ---------------------------------------------------- |
| 1    | Foundation + core services     | Working user/course APIs, per-service schema, local setup |
| 2    | Enrollment + publishing        | Reliable enrollment; Temporal publishing workflow    |
| 3    | Events + observability         | Kafka + Celery flows; metrics, logs, tracing         |

## 6. Traceability (feature → requirement → deliverable)

| Feature (Week 1)         | Requirement | Deliverable                                             |
| ------------------------ | ----------- | ------------------------------------------------------- |
| User registration/login  | FR-1, FR-2  | `user-service` `/auth/*`, `security.py`, `User` model   |
| Roles & authorization    | FR-4        | `deps.py` guards, ownership checks in `courses.py`      |
| Course CRUD + lifecycle   | FR-3        | `course-service` `/courses/*`, `Course` model + status  |
| Per-service data ownership| NFR-1       | Separate `users_db` / `courses_db`; soft `instructor_id`|
| Stateless auth            | NFR-6       | Shared-secret JWT, verified locally per service         |
| Local runnability         | NFR-7       | `docker-compose.yml`, per-service `Dockerfile`          |

## 7. Out of scope for Week 1

Enrollment, publishing workflow, modules/lessons content, events, analytics, and
observability tooling — all planned and sequenced above, not built yet.
