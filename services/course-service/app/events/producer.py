"""Kafka producer for course domain events.

JSON events (Schema Registry present as the seam for Avro later). Publishing is
best-effort and MUST NOT fail the request that triggered it.
"""
import atexit
import json
import logging

from confluent_kafka import Producer

from app.core.config import settings

logger = logging.getLogger(settings.SERVICE_NAME)

_producer: Producer | None = None


def _get_producer() -> Producer:
    global _producer
    if _producer is None:
        _producer = Producer({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS})
        atexit.register(_flush)
    return _producer


def _flush() -> None:
    if _producer is not None:
        _producer.flush(5)


def _on_delivery(err, msg) -> None:
    if err is not None:
        logger.error("Event delivery failed: %s", err)
    else:
        logger.info("Event delivered to %s [%s]", msg.topic(), msg.partition())


def publish_event(topic: str, key: str, event: dict) -> None:
    try:
        p = _get_producer()
        p.produce(
            topic,
            key=key.encode("utf-8"),
            value=json.dumps(event).encode("utf-8"),
            on_delivery=_on_delivery,
        )
        p.poll(0)
    except Exception as exc:  # never fail the request because of eventing
        logger.error("Failed to publish event to %s: %s", topic, exc)
