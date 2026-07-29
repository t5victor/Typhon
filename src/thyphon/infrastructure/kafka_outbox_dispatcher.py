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
        for record in self.store.unpublished_events():
            body = str(record.event.payload()).encode()
            self.publish("thyphon.domain-events", record.stream_id, body)
            if duplicate_first and delivered == 0:
                self.publish("thyphon.domain-events", record.stream_id, body)
            self.store.mark_published(record.event.event_id)
            delivered += 1
        return delivered
