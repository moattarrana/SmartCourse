"""Gateway configuration: where the downstream services live."""
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    SERVICE_NAME: str = "api-gateway"
    USER_SERVICE_URL: str = "http://localhost:8001"
    COURSE_SERVICE_URL: str = "http://localhost:8002"
    REQUEST_TIMEOUT_SECONDS: float = 30.0


settings = Settings()
