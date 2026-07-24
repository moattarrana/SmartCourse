"""Smoke test for the health endpoint.

Deliberately does NOT enter the TestClient context manager, so the lifespan
(which waits for Postgres and creates tables) never runs. This keeps the test
a fast unit check that needs no database.
"""
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_health_ok():
    resp = client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["service"] == "user-service"
