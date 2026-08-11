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


settings = Settings()
