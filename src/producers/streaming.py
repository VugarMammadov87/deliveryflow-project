"""Kafka transport and drift-resistant pacing for synthetic stream events."""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from random import Random
from typing import Any

from producers.config import ProducerConfig
from producers.events import build_event, build_vehicle_telemetry_event

LOG = logging.getLogger("deliveryflow.producer")


class FixedIntervalPacer:
    """Pace repeated work against monotonic deadlines to avoid rate drift."""

    def __init__(
        self,
        interval_seconds: float,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        """Create a pacer whose first deadline is one interval from now."""
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be greater than zero")
        self._interval_seconds = interval_seconds
        self._monotonic = monotonic
        self._sleep = sleep
        self._next_deadline = monotonic()

    def wait(self) -> None:
        """Wait until the next fixed deadline, skipping sleep when behind."""
        self._next_deadline += self._interval_seconds
        remaining_seconds = self._next_deadline - self._monotonic()
        if remaining_seconds > 0:
            self._sleep(remaining_seconds)


def _new_kafka_producer(config: ProducerConfig) -> Any:
    """Create the configured Kafka client while keeping imports optional."""
    from kafka import KafkaProducer

    return KafkaProducer(
        bootstrap_servers=config.bootstrap_servers,
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=50,
    )


def _publish_events(
    config: ProducerConfig,
    rng: Random,
    *,
    topic: str,
    event_factory: Callable[[int, Random], dict[str, Any]],
    key_field: str,
) -> None:
    """Publish finite or continuous events through the shared Kafka path."""
    producer = _new_kafka_producer(config)
    pacer = FixedIntervalPacer(config.continuous_interval_seconds) if config.continuous else None
    index = 0

    try:
        while config.continuous or index < config.event_count:
            batch_size = config.continuous_records_per_interval if config.continuous else 1
            for _ in range(batch_size):
                if not config.continuous and index >= config.event_count:
                    break

                event = event_factory(index, rng)
                event_key = event[key_field]
                producer.send(topic, key=event_key, value=event)
                LOG.info(
                    "published topic=%s event_id=%s event_type=%s key=%s",
                    topic,
                    event["event_id"],
                    event["event_type"],
                    event_key,
                )
                index += 1

            producer.flush(timeout=30)
            if pacer is not None:
                pacer.wait()
            elif index < config.event_count:
                time.sleep(config.interval_seconds)
    finally:
        producer.flush(timeout=30)
        producer.close(timeout=10)


def publish_stream_events(config: ProducerConfig, rng: Random) -> None:
    """Publish generated delivery events for the Delivery Flink application."""
    _publish_events(
        config,
        rng,
        topic=config.topic,
        event_factory=build_event,
        key_field="delivery_id",
    )


def publish_fleet_telemetry_events(config: ProducerConfig, rng: Random) -> None:
    """Publish generated vehicle telemetry for the Fleet Flink application."""
    _publish_events(
        config,
        rng,
        topic=config.fleet_topic,
        event_factory=build_vehicle_telemetry_event,
        key_field="vehicle_id",
    )
