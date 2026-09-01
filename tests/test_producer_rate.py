"""Rate and pacing tests for the continuous synthetic producer."""

from __future__ import annotations

from random import Random
from typing import Any

import pytest

from producers.config import ProducerConfig
from producers import streaming
from producers.streaming import FixedIntervalPacer


def test_continuous_default_emits_ten_events_per_sixty_seconds(monkeypatch: pytest.MonkeyPatch) -> None:
    """Protect the default ten-events-per-minute throughput contract."""
    monkeypatch.delenv("PRODUCER_CONTINUOUS_INTERVAL_SECONDS", raising=False)
    monkeypatch.delenv("PRODUCER_EVENTS_PER_INTERVAL", raising=False)

    config = ProducerConfig.from_env()

    assert config.continuous_interval_seconds == pytest.approx(60.0)
    assert config.continuous_records_per_interval == 10


def test_continuous_rate_environment_overrides_are_preserved(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep interval and records-per-interval as public environment overrides."""
    monkeypatch.setenv("PRODUCER_CONTINUOUS_INTERVAL_SECONDS", "2.5")
    monkeypatch.setenv("PRODUCER_EVENTS_PER_INTERVAL", "4")

    config = ProducerConfig.from_env()

    assert config.continuous_interval_seconds == pytest.approx(2.5)
    assert config.continuous_records_per_interval == 4


def test_continuous_interval_publishes_one_batch_before_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify one continuous interval publishes the configured event batch."""
    sent: list[tuple[str, str, dict[str, Any]]] = []
    flush_count = 0
    close_count = 0

    class FakeProducer:
        def send(self, topic: str, *, key: str, value: dict[str, Any]) -> None:
            sent.append((topic, key, value))

        def flush(self, timeout: int) -> None:
            nonlocal flush_count
            flush_count += 1

        def close(self, timeout: int) -> None:
            nonlocal close_count
            close_count += 1

    class StopAfterFirstInterval:
        def __init__(self, config: ProducerConfig) -> None:
            self.config = config

        def wait(self) -> None:
            object.__setattr__(self.config, "continuous", False)

    config = ProducerConfig.from_env()
    object.__setattr__(config, "continuous", True)
    object.__setattr__(config, "event_count", 10)

    monkeypatch.setattr(streaming, "_new_kafka_producer", lambda _: FakeProducer())
    monkeypatch.setattr(streaming, "FixedIntervalPacer", lambda *_: StopAfterFirstInterval(config))

    streaming._publish_events(
        config,
        Random(42),
        topic="delivery-events",
        event_factory=lambda index, _: {
            "event_id": f"event-{index}",
            "event_type": "TEST",
            "delivery_id": f"delivery-{index}",
        },
        key_field="delivery_id",
    )

    assert len(sent) == 10
    assert [key for _, key, _ in sent] == [f"delivery-{index}" for index in range(10)]
    assert flush_count >= 2
    assert close_count == 1


def test_fixed_interval_pacer_compensates_for_publish_time() -> None:
    """Verify ten paced emissions occupy one minute without cumulative drift."""
    clock = {"now": 0.0}
    sleep_calls: list[float] = []

    def monotonic() -> float:
        """Return the deterministic test clock."""
        return clock["now"]

    def sleep(seconds: float) -> None:
        """Advance the deterministic clock instead of waiting in real time."""
        sleep_calls.append(seconds)
        clock["now"] += seconds

    pacer = FixedIntervalPacer(60.0, monotonic=monotonic, sleep=sleep)

    for _ in range(2):
        clock["now"] += 0.25
        pacer.wait()

    assert len(sleep_calls) == 2
    assert sleep_calls == pytest.approx([59.75, 59.75])
    assert clock["now"] == pytest.approx(120.0)
