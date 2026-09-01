"""Low-level Kafka transport shared by every event producer.

Opens one producer connection (lazy singleton) and exposes publish(). Each
domain producer (course_events, enrollment_events) builds its own event dict
and hands it here. Publishing to Kafka is shared-infrastructure access, not
database access, so the worker still never touches any service's database.
"""
import atexit
import json
import logging

from confluent_kafka import Producer

from app.config import KAFKA_BOOTSTRAP_SERVERS

logger = logging.getLogger("temporal-worker.events")

_producer: Producer | None = None


def _get_producer() -> Producer:
    global _producer
    if _producer is None:
        _producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})  #create the producer, telling it Kafka's address.
        atexit.register(_flush)
    return _producer


def _flush() -> None: #don't lose buffered messages on shutdown
    if _producer is not None:
        _producer.flush(5)


def _on_delivery(err, msg) -> None:
    if err is not None:
        logger.error("Event delivery failed: %s", err)
    else:
        logger.info("Event delivered to %s [%s]", msg.topic(), msg.partition())


def publish(topic: str, key: str, event: dict) -> None:
    """Send one JSON event to a Kafka topic. Best-effort, logs on failure."""
    p = _get_producer()
    p.produce(
        topic,
        key=key.encode("utf-8"),
        value=json.dumps(event).encode("utf-8"),
        on_delivery=_on_delivery,
    )
    p.poll(0)
