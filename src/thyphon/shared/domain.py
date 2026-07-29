from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Protocol
import hashlib
import json
from uuid import UUID, uuid4


class DomainViolation(Exception):
    """Raised when an intention cannot be honoured by current aggregate facts."""


class OptimisticConcurrencyConflict(Exception):
    """A different command already advanced the stream."""


class IdempotencyKeyReused(Exception):
    """The client reused a key for a materially different command."""


class ProviderReferenceAlreadyObserved(Exception):
    """A payment-provider reference is globally bound to a previous settlement."""


class SettlementAlreadyRequestedForWinningBid(Exception):
    """A winning bid can cause one and only one settlement stream."""


class InvalidSettlementCausation(Exception):
    """A Settlement must be caused by the canonical winning bid of its Auction."""


@dataclass(frozen=True)
class CommandContext:
    """Delivery metadata kept outside the domain intention.

    The command describes *what* a business actor wants.  Retry, tracing and
    tenancy describe how that intention arrived and must never become part of
    a domain fact's payload.
    """

    idempotency_key: str
    correlation_id: str
    causation_id: str | None = None
    actor_id: str | None = None
    tenant_id: str | None = None


def command_metadata(command: Any) -> tuple[str, str]:
    """Stable content fingerprint for a domain intention only."""
    payload = canonical_json_value(asdict(command))
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return type(command).__name__, hashlib.sha256(encoded.encode()).hexdigest()


def canonical_json_value(value: Any) -> Any:
    """Encode command values deterministically without transport-specific casts."""
    if isinstance(value, (Decimal, UUID)):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, list):
        return [canonical_json_value(item) for item in value]
    if isinstance(value, tuple):
        return [canonical_json_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): canonical_json_value(item) for key, item in value.items()}
    return value


def stream_key(aggregate_type: str, aggregate_id: str) -> str:
    """Namespace every stream so unrelated aggregates can never share a history."""
    if not aggregate_id or ":" in aggregate_id:
        raise ValueError("aggregate ids must be non-empty and cannot contain ':'")
    return f"{aggregate_type}:{aggregate_id}"


def aggregate_id(stream_id: str, aggregate_type: str) -> str:
    prefix = f"{aggregate_type}:"
    if not stream_id.startswith(prefix):
        raise ValueError(f"expected a {aggregate_type} stream, got {stream_id}")
    return stream_id.removeprefix(prefix)


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
    global_position: int | None = None
    schema_version: int = 1


class EventSourcedAggregate(Protocol):
    stream_id: str
    version: int

    def pull_uncommitted_events(self) -> list[DomainEvent]: ...


class EventStore(Protocol):
    def read_stream(self, stream_id: str) -> list[RecordedEvent]: ...

    def idempotency_result(
        self, idempotency_key: str, *, stream_id: str, command_name: str, request_hash: str,
        actor_id: str | None, tenant_id: str | None,
    ) -> int | None: ...

    def append(
        self,
        *,
        stream_id: str,
        expected_version: int,
        events: list[DomainEvent],
        idempotency_key: str,
        command_name: str,
        request_hash: str,
        context: CommandContext,
    ) -> int: ...
