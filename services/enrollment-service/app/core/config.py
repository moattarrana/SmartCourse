"""Enrollment service configuration.

JWT_SECRET must match the user service so tokens verify here. This service
validates that a course is published by calling the course service.
"""
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    SERVICE_NAME: str = "enrollment-service"

    DATABASE_URL: str = "postgresql+psycopg2://smartcourse:smartcourse@localhost:5434/enrollments_db"

    JWT_SECRET: str = "change-me-in-production"
    JWT_ALGORITHM: str = "HS256"

    COURSE_SERVICE_URL: str = "http://course-service:8000"
    REQUEST_TIMEOUT_SECONDS: float = 5.0

    # Enrollment rules
    MAX_ENROLLMENTS_PER_COURSE: int = 10

    # Shared secret the Temporal worker uses to call internal endpoints.
    INTERNAL_API_KEY: str = "change-me-internal-key"

    # Temporal (enrollment workflow)
    TEMPORAL_HOST: str = "temporal:7233"
    TEMPORAL_NAMESPACE: str = "default"
    TEMPORAL_TASK_QUEUE: str = "enrollment-processing"

    # Kafka (event publishing)
    KAFKA_BOOTSTRAP_SERVERS: str = "kafka:9092"
    KAFKA_ENROLLMENT_TOPIC: str = "enrollment.events"
    KAFKA_PROGRESS_TOPIC: str = "progress.events"

    # Observability (OpenTelemetry -> Jaeger via OTLP/HTTP)
    OTEL_EXPORTER_OTLP_ENDPOINT: str = "http://jaeger:4318"


settings = Settings()
