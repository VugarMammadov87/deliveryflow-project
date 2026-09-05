"""Synthetic producer entrypoint and backward-compatible public imports.

Docker Compose and Make targets keep invoking this module. The implementation
is split across focused modules while event builders remain re-exported here so
existing tests and callers do not need an immediate import migration.
"""

from __future__ import annotations

import logging
from random import Random

from producers.config import ProducerConfig, SourceDbConfig
from producers.events import build_event, build_vehicle_telemetry_event
from producers.source import seed_batch_source
from producers.streaming import publish_fleet_telemetry_events, publish_stream_events
from producers.transportation_costs import build_transportation_cost

__all__ = [
    "ProducerConfig",
    "SourceDbConfig",
    "build_event",
    "build_transportation_cost",
    "build_vehicle_telemetry_event",
    "main",
    "publish_fleet_telemetry_events",
    "publish_stream_events",
    "seed_batch_source",
]


def main() -> None:
    """Run the configured Delivery or Fleet producer workflow."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    config = ProducerConfig.from_env()
    rng = Random(42)

    if config.app == "fleet":
        publish_fleet_telemetry_events(config, rng)
        return

    if config.mode in {"batch", "both"}:
        seed_batch_source(config.batch_rows, rng)

    if config.mode in {"stream", "both"}:
        publish_stream_events(config, rng)


if __name__ == "__main__":
    main()
