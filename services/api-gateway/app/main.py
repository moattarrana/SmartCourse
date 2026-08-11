"""A minimal API gateway / reverse proxy.

Responsibilities kept intentionally small for Week 1:
  - one public entry point for clients
  - path-based routing to the right downstream service
  - transparent forwarding of method, headers (incl. Authorization), body, query

It does NOT terminate auth: each downstream service validates the JWT itself.
That keeps services independently deployable and testable. Cross-cutting
concerns (rate limiting, central auth, tracing) can be added here later.

Routing:
  /api/auth/*    and /api/users/*    -> user service
  /api/courses/*                     -> course service
"""
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

from app.config import settings

# Persistent async client (connection pooling), managed by the app lifespan.
_client: httpx.AsyncClient | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _client
    _client = httpx.AsyncClient(timeout=settings.REQUEST_TIMEOUT_SECONDS)
    yield
    await _client.aclose()


app = FastAPI(title="SmartCourse API Gateway", version="0.1.0", lifespan=lifespan)


# Longest prefix first so /api/users doesn't get swallowed by a broader rule.
_ROUTES: list[tuple[str, str]] = [
    ("/api/auth", settings.USER_SERVICE_URL),
    ("/api/users", settings.USER_SERVICE_URL),
    ("/api/courses", settings.COURSE_SERVICE_URL),
    ("/api/enrollments", settings.ENROLLMENT_SERVICE_URL),
]

# Hop-by-hop headers must not be forwarded (RFC 7230 sec 6.1).
_HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade", "host", "content-length",
}


def _resolve(path: str) -> tuple[str, str] | None:
    for prefix, base in _ROUTES:
        if path == prefix or path.startswith(prefix + "/"):
            downstream_path = path[len("/api"):]  # strip the /api gateway prefix
            return base, downstream_path
    return None


@app.get("/health", tags=["meta"])
async def health() -> dict:
    return {"status": "ok", "service": settings.SERVICE_NAME}


@app.api_route(
    "/api/{full_path:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
)
async def proxy(full_path: str, request: Request) -> Response:
    assert _client is not None
    resolved = _resolve(request.url.path)
    if resolved is None:
        return JSONResponse(status_code=404, content={"detail": "No route for path"})

    base_url, downstream_path = resolved
    body = await request.body()
    fwd_headers = {
        k: v for k, v in request.headers.items() if k.lower() not in _HOP_BY_HOP
    }

    try:
        upstream = await _client.request(
            method=request.method,
            url=f"{base_url}{downstream_path}",
            params=request.query_params.multi_items(),
            headers=fwd_headers,
            content=body,
        )
    except httpx.ConnectError:
        return JSONResponse(
            status_code=502, content={"detail": "Downstream service unavailable"}
        )
    except httpx.TimeoutException:
        return JSONResponse(
            status_code=504, content={"detail": "Downstream service timed out"}
        )

    resp_headers = {
        k: v for k, v in upstream.headers.items() if k.lower() not in _HOP_BY_HOP
    }
    return Response(
        content=upstream.content,
        status_code=upstream.status_code,
        headers=resp_headers,
        media_type=upstream.headers.get("content-type"),
    )
