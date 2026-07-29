from __future__ import annotations

import json
from typing import Any


class KafkaPublisher:
    """Kafka adapter used by the outbox worker, preserving at-least-once publication semantics."""

    def __init__(self, bootstrap_servers: str) -> None:
        self.bootstrap_servers = bootstrap_servers

    async def publish(self, topic: str, partition_key: str, envelope: dict[str, Any]) -> None:
        try:
            from aiokafka import AIOKafkaProducer
        except ImportError as error:  # pragma: no cover - requires optional runtime dependency
            raise RuntimeError("Install Thyphon with the 'runtime' extra for Kafka support") from error
        producer = AIOKafkaProducer(bootstrap_servers=self.bootstrap_servers, acks="all")
        await producer.start()
        try:
            await producer.send_and_wait(
                topic,
                key=partition_key.encode(),
                value=json.dumps(envelope, sort_keys=True).encode(),
            )
        finally:
            await producer.stop()
