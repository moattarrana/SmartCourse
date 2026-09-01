
import os

COURSE_SERVICE_URL = os.getenv("COURSE_SERVICE_URL", "http://course-service:8000")
ENROLLMENT_SERVICE_URL = os.getenv(
    "ENROLLMENT_SERVICE_URL", "http://enrollment-service:8000"
)
INTERNAL_API_KEY = os.getenv("INTERNAL_API_KEY", "change-me-internal-key")

TEMPORAL_HOST = os.getenv("TEMPORAL_HOST", "temporal:7233")
TEMPORAL_NAMESPACE = os.getenv("TEMPORAL_NAMESPACE", "default")

# Publishing workflow task queue (existing).
TASK_QUEUE = os.getenv("TEMPORAL_TASK_QUEUE", "course-publishing")
# Enrollment workflow task queue (new).
ENROLLMENT_TASK_QUEUE = os.getenv("ENROLLMENT_TASK_QUEUE", "enrollment-processing")

# --- Kafka: the worker now publishes the two workflow events directly ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_COURSE_TOPIC = os.getenv("KAFKA_COURSE_TOPIC", "course.events")
KAFKA_ENROLLMENT_TOPIC = os.getenv("KAFKA_ENROLLMENT_TOPIC", "enrollment.events")

# --- Celery: the worker enqueues the welcome notification directly ---
CELERY_BROKER_URL = os.getenv(
    "CELERY_BROKER_URL", "amqp://smartcourse:smartcourse@rabbitmq:5672//"
)
CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "redis://redis:6379/0")
NOTIFICATION_TASK_NAME = os.getenv("NOTIFICATION_TASK_NAME", "send_welcome_notification")
