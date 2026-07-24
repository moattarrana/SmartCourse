# SmartCourse — Technical Design (Part A, Week 1)

## 1. Service / module breakdown

Three deployable services, each a small FastAPI app.

### API Gateway (`services/api-gateway`)
Single public entry point. Routes `/api/*` to the correct service by path prefix,
forwarding method, headers (including `Authorization`), query, and body
transparently. It does **not** terminate authentication — services do that
themselves. This is the natural home for future cross-cutting concerns (rate
limiting, request tracing, central auth) but is intentionally thin for now.

### User Service (`services/user-service`) → `users_db`
Owns identity. Registration, login, and user reads/updates. Issues JWTs and hashes
passwords with bcrypt. Internal layers:
- `api/routes/` — `auth.py` (register, login), `users.py` (me, get, list, update)
- `api/deps.py` — `get_current_user` and role guards
- `services/user_service.py` — business rules (unique email, authentication)
- `models/user.py` — `User` + `UserRole` enum
- `schemas/` — request/response DTOs (never expose the password hash)
- `core/` — config, DB session, security primitives

### Course Service (`services/course-service`) → `courses_db`
Owns courses. Full CRUD with a lifecycle `status` (draft → published → archived).
Only **verifies** JWTs — it never issues them. Same internal layering as the user
service. `require_instructor` gates writes; ownership is checked on edit/delete.

## 2. Data model

`users` (users_db): `id (uuid pk)`, `email (unique)`, `hashed_password`,
`full_name`, `role`, `is_active`, `created_at`, `updated_at`.

`courses` (courses_db): `id (uuid pk)`, `title`, `description`,
`instructor_id (uuid, indexed)`, `status`, `created_at`, `updated_at`.

`instructor_id` references a user in a *different* database. There is no SQL
foreign key across that boundary — that is a deliberate microservices choice, not
an oversight (see §5).

## 3. Data flow

Create-a-course, end to end:

1. Client authenticates at the user service (`/auth/login`) and receives a JWT
   whose claims include `sub` (user id), `role`, and `email`.
2. Client calls `POST /api/courses` through the gateway with the token.
3. Gateway forwards the request unchanged to the course service.
4. Course service decodes the JWT locally, confirms `role == instructor`,
   and takes `instructor_id` from the token's `sub` — no call back to the user
   service.
5. Course is written to `courses_db` with `status = draft`; the response is
   proxied back through the gateway.

## 4. Event flow (planned, Weeks 2–3)

Today all flows are synchronous request/response. The seams for asynchronous,
event-driven behavior are already in place:
- `Course.status` gives the publishing workflow (Temporal) explicit states to
  transition between without schema change.
- UUID identifiers let events reference entities across services safely.
- Clear service boundaries mean "enrollment created" / "course published" become
  Kafka topics consumed by Celery workers for analytics, notifications, and
  content indexing — none of which block the user-facing request.

## 5. Key design decisions

- **Microservices over a monolith.** For Week 1's CRUD alone this is more
  structure than strictly needed, but Part A is fundamentally about independent,
  event-driven services (Temporal, Kafka, Celery). Starting service-oriented means
  those slot in without a rewrite.
- **Database per service.** Each service is the sole owner of its data and the
  only thing that connects to its database. This is what lets services scale,
  deploy, and fail independently.
- **No cross-service foreign keys.** `instructor_id` is a soft reference. Referential
  integrity across services is maintained by trusting the authenticated token now,
  and by events later — not by a database constraint that cannot span two databases.
- **Stateless shared-secret JWT.** The user service signs; every service verifies
  with the same secret. Authorization needs no synchronous lookup, avoiding chatty
  coupling and a single point of failure on every request.
- **UUID primary keys.** No shared sequence to coordinate; services mint IDs
  independently — the right call for a distributed system.
- **Layered services (api → services → models).** Route handlers stay thin;
  business rules live in a testable service layer.
- **`create_all` now, Alembic in Week 2.** The schema is stable this week.
  Versioned migrations start paying off once enrollment and content models begin
  to churn it.

## 6. Assumptions & tradeoffs

- **Shared Postgres credentials / instance locally.** For dev convenience both
  databases run in Postgres containers with the same credentials. In production
  they would be separate instances with separate secrets; the *logical* ownership
  boundary is already enforced (a service only ever talks to its own DB).
- **Gateway does not centralize auth.** Simpler and keeps services independently
  testable; the tradeoff is that auth logic is verified in each service (minimal,
  since verification is a few lines). Centralizing it is a deliberate later option.
- **Symmetric JWT (HS256).** One shared secret is simple and fine for a trusted
  internal mesh. If services were operated by different teams/trust zones,
  asymmetric signing (RS256, user service holds the private key) would be safer.
- **Token invalidation.** Tokens are valid until they expire; there is no
  revocation list yet. Acceptable for Week 1; a Redis denylist is a later option.
- **No inter-service validation of `instructor_id`.** We trust the token that the
  id is a real instructor. If a user were deleted, existing courses would still
  reference the old id until a "user deleted" event reconciles them (Week 3).
