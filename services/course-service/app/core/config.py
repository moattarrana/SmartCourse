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


settings = Settings()
