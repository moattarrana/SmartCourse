"""Analytics service configuration."""
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    SERVICE_NAME: str = "analytics-service"

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = "kafka:9092"
    KAFKA_ENROLLMENT_TOPIC: str = "enrollment.events"
    KAFKA_COURSE_TOPIC: str = "course.events"
    KAFKA_PROGRESS_TOPIC: str = "progress.events"
    KAFKA_USER_TOPIC: str = "user.events"
    KAFKA_GROUP_ID: str = "analytics-service"

    # MongoDB (NoSQL analytics store)
    MONGO_URL: str = "mongodb://mongo:27017"
    MONGO_DB: str = "analytics_db"

    # Celery (RabbitMQ broker, Redis result backend)
    CELERY_BROKER_URL: str = "amqp://smartcourse:smartcourse@rabbitmq:5672//"
    CELERY_RESULT_BACKEND: str = "redis://redis:6379/0"

    # Email (Mailpit dev SMTP — catches mail locally, nothing sent externally)
    SMTP_HOST: str = "mailpit"
    SMTP_PORT: int = 1025
    EMAIL_FROM: str = "noreply@smartcourse.local"


settings = Settings()
