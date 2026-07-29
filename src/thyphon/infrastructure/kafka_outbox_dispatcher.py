from __future__ import annotations

from collections.abc import Callable

from thyphon.infrastructure.sqlite_event_store import SqliteEventStore


class KafkaOutboxDispatcher:
    """Publishes committed outbox facts; injected publish makes the delivery semantics testable."""

    def __init__(self, store: SqliteEventStore, publish: Callable[[str, str, bytes], None]) -> None:
        self.store = store
        self.publish = publish

    def deliver_pending(self, duplicate_first: bool = False) -> int:
        delivered = 0
        for topic, partition_key, event_id, body in self.store.unpublished_outbox():
            self.publish(topic, partition_key, body)
            if duplicate_first and delivered == 0:
                self.publish(topic, partition_key, body)
            self.store.mark_published(event_id)
            delivered += 1
        return delivered
