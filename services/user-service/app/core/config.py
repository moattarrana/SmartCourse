"""Application configuration, loaded from environment variables.

Using pydantic-settings keeps config typed and validated in one place, instead
of scattering os.getenv() calls through the codebase.
"""
from pydantic_settings import BaseSettings, SettingsConfigDict

# sqlalchemy URL format: dialect+driver://username:password@host:port/database
#sqlalchemy is the engine to talk to database
class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Service metadata
    SERVICE_NAME: str = "user-service"

    # Database (this service owns its own DB — nothing else connects here)
    DATABASE_URL: str = "postgresql+psycopg2://smartcourse:smartcourse@localhost:5432/users_db"

    # JWT: the SECRET is shared with course-service so it can verify tokens
    # issued here without calling back to us. Keep it identical across services.
    JWT_SECRET: str = "change-me-in-production"
    JWT_ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60


settings = Settings()
