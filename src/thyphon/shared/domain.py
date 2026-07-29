from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Protocol
from uuid import UUID, uuid4


class DomainViolation(Exception):
    """Raised when an intention cannot be honoured by current aggregate facts."""


class OptimisticConcurrencyConflict(Exception):
    """A different command already advanced the stream."""


@dataclass(frozen=True, kw_only=True)
class DomainEvent:
    event_id: UUID
    occurred_at: datetime

    @classmethod
    def now(cls, **data: Any) -> DomainEvent:
        return cls(event_id=uuid4(), occurred_at=datetime.now(UTC), **data)

    @property
    def event_name(self) -> str:
        return type(self).__name__

    def payload(self) -> dict[str, Any]:
        def encode(value: Any) -> Any:
            if isinstance(value, Decimal):
                return str(value)
            if isinstance(value, UUID):
                return str(value)
            if isinstance(value, datetime):
                return value.isoformat()
            return value
        return {key: encode(value) for key, value in asdict(self).items()}


@dataclass(frozen=True)
class RecordedEvent:
    stream_id: str
    stream_version: int
    event: DomainEvent


class EventSourcedAggregate(Protocol):
    stream_id: str
    version: int

    def pull_uncommitted_events(self) -> list[DomainEvent]: ...


class EventStore(Protocol):
    def read_stream(self, stream_id: str) -> list[RecordedEvent]: ...

    def idempotency_result(self, idempotency_key: str) -> int | None: ...

    def append(
        self,
        *,
        stream_id: str,
        expected_version: int,
        events: list[DomainEvent],
        idempotency_key: str,
    ) -> int: ...
