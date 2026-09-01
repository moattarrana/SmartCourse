"""Course service configuration.

Note JWT_SECRET must match the user service so tokens issued there verify here.
This service never creates tokens — it only decodes and trusts them.
"""
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    SERVICE_NAME: str = "course-service"

    DATABASE_URL: str = "postgresql+psycopg2://smartcourse:smartcourse@localhost:5433/courses_db"

    JWT_SECRET: str = "change-me-in-production"
    JWT_ALGORITHM: str = "HS256"

    # Temporal (course publishing workflow)
    TEMPORAL_HOST: str = "temporal:7233"
    TEMPORAL_NAMESPACE: str = "default"
    TEMPORAL_TASK_QUEUE: str = "course-publishing"

    # Shared secret the Temporal worker uses to call internal endpoints.
    INTERNAL_API_KEY: str = "change-me-internal-key"

    # Observability (OpenTelemetry -> Jaeger via OTLP/HTTP)
    OTEL_EXPORTER_OTLP_ENDPOINT: str = "http://jaeger:4318"

    # Kafka (event publishing)
    KAFKA_BOOTSTRAP_SERVERS: str = "kafka:9092"
    KAFKA_COURSE_TOPIC: str = "course.events"


settings = Settings()
