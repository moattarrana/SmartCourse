"""Gateway configuration: where the downstream services live."""
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    SERVICE_NAME: str = "api-gateway"
    USER_SERVICE_URL: str = "http://localhost:8001"
    COURSE_SERVICE_URL: str = "http://localhost:8002"
    ENROLLMENT_SERVICE_URL: str = "http://enrollment-service:8000"
    REQUEST_TIMEOUT_SECONDS: float = 30.0
    OTEL_EXPORTER_OTLP_ENDPOINT: str = "http://jaeger:4318"


settings = Settings()
