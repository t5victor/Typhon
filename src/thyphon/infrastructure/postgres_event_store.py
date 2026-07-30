from __future__ import annotations

import json
from datetime import UTC, datetime
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterator, Mapping
from uuid import UUID

from thyphon.infrastructure.sqlite_event_store import EVENT_SCHEMA_VERSIONS, EVENT_TYPES, _decode_value, upcast_event
from thyphon.shared.domain import CommandContext, DomainEvent, IdempotencyKeyReused, InvalidSettlementCausation, OptimisticConcurrencyConflict, ProviderReferenceAlreadyObserved, RecordedEvent, SettlementAlreadyRequestedForWinningBid


@dataclass(frozen=True)
class CanonicalEvent:
    """An event proved to be identical to its immutable Event Store record."""

    recorded: RecordedEvent
    correlation_id: str
    causation_id: str | None
    actor_id: str | None
    tenant_id: str | None


class CanonicalEventDecodeError(ValueError):
    """The Event Store proved an event is authentic, but this reader cannot decode it.

    Keeping the canonical ID lets the worker quarantine the fact in the
    repairable failure lane. Treating it as arbitrary broker input would make
    a rolling schema upgrade require a full rebuild merely to retry one row.
    """

    def __init__(self, event_id: UUID, error: ValueError) -> None:
        super().__init__(str(error))
        self.event_id = event_id


_ENVELOPE_FIELDS = frozenset({
    "event_id", "event_name", "schema_version", "stream_id", "stream_version", "global_position",
    "occurred_at", "payload", "correlation_id", "causation_id", "actor_id", "tenant_id",
})


def _require_envelope_shape(envelope: Mapping[str, Any]) -> UUID:
    if set(envelope) != _ENVELOPE_FIELDS:
        raise ValueError("domain-event envelope has an unknown or missing field")
    try:
        event_id = UUID(str(envelope["event_id"]))
    except (TypeError, ValueError, KeyError) as error:
        raise ValueError("domain-event envelope has an invalid event_id") from error
    if (
        not isinstance(envelope["event_name"], str)
        or not isinstance(envelope["schema_version"], int) or isinstance(envelope["schema_version"], bool)
        or not isinstance(envelope["stream_id"], str) or not envelope["stream_id"]
        or not isinstance(envelope["stream_version"], int) or isinstance(envelope["stream_version"], bool) or envelope["stream_version"] < 1
        or not isinstance(envelope["global_position"], int) or isinstance(envelope["global_position"], bool) or envelope["global_position"] < 1
        or not isinstance(envelope["occurred_at"], str)
        or not isinstance(envelope["payload"], dict)
        or not isinstance(envelope["correlation_id"], str) or not envelope["correlation_id"]
        or any(envelope[name] is not None and not isinstance(envelope[name], str) for name in ("causation_id", "actor_id", "tenant_id"))
    ):
        raise ValueError("domain-event envelope has invalid field types")
    if envelope["payload"].get("event_id") != str(event_id):
        raise ValueError("domain-event envelope event_id does not match its payload")
    try:
        occurred_at = datetime.fromisoformat(envelope["occurred_at"])
    except ValueError as error:
        raise ValueError("domain-event envelope has an invalid occurred_at") from error
    if occurred_at.tzinfo is None:
        raise ValueError("domain-event envelope occurred_at must include a timezone")
    return event_id


class PostgresEventStore:
    """Production adapter: event append and transactional outbox share one PostgreSQL transaction.

    `psycopg` is intentionally imported only at construction so domain/Bazel tests remain hermetic.
    """

    def __init__(self, connection_string: str) -> None:
        try:
            from psycopg_pool import ConnectionPool
        except ImportError as error:  # pragma: no cover - exercised by runtime setup
            raise RuntimeError("Install Thyphon with the 'runtime' extra for PostgreSQL support") from error
        self.pool = ConnectionPool(conninfo=connection_string, min_size=1, max_size=12, open=True)

    @contextmanager
    def connection(self) -> Iterator[Any]:
        with self.pool.connection() as connection:
            yield connection

    def close(self) -> None:
        self.pool.close()

    def idempotency_result(self, idempotency_key: str, *, stream_id: str, command_name: str, request_hash: str, actor_id: str | None, tenant_id: str | None) -> int | None:
        with self.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT stream_id, command_name, request_hash, actor_id, tenant_id, resulting_version FROM command_receipt WHERE idempotency_key = %s", (idempotency_key,)
            )
            row = cursor.fetchone()
        if row is None:
            return None
        if tuple(row[:5]) != (stream_id, command_name, request_hash, actor_id or "", tenant_id or ""):
            raise IdempotencyKeyReused("idempotency key was already used for a different command")
        return int(row[5])

    def read_stream(self, stream_id: str) -> list[RecordedEvent]:
        with self.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT stream_id, stream_version, event_name, payload, global_position, schema_version FROM event_stream "
                "WHERE stream_id = %s ORDER BY stream_version", (stream_id,)
            )
            rows = cursor.fetchall()
        return [self._recorded(*row) for row in rows]

    def canonical_event(self, envelope: Mapping[str, Any]) -> CanonicalEvent:
        """Reject broker input unless it exactly represents an Event Store fact.

        Kafka is a delivery mechanism, not an authority able to mint financial
        obligations. The process manager consumes only the canonical row.
        """
        event_id = _require_envelope_shape(envelope)
        with self.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT event_id, stream_id, stream_version, event_name, payload, global_position, schema_version, "
                "occurred_at, correlation_id, causation_id, actor_id, tenant_id "
                "FROM event_stream WHERE event_id=%s",
                (event_id,),
            )
            row = cursor.fetchone()
        if row is None:
            raise ValueError("domain-event envelope is not present in the Event Store")
        (
            stored_id, stream_id, stream_version, event_name, payload, global_position, schema_version,
            occurred_at, correlation_id, causation_id, actor_id, tenant_id,
        ) = row
        comparable = {
            "event_id": str(stored_id), "event_name": event_name, "schema_version": int(schema_version),
            "stream_id": stream_id, "stream_version": int(stream_version), "global_position": int(global_position),
            "payload": payload, "correlation_id": correlation_id, "causation_id": causation_id,
            "actor_id": actor_id, "tenant_id": tenant_id,
        }
        supplied = {key: envelope[key] for key in comparable}
        if json.dumps(supplied, sort_keys=True, separators=(",", ":"), default=str) != json.dumps(comparable, sort_keys=True, separators=(",", ":"), default=str):
            raise ValueError("domain-event envelope differs from its Event Store fact")
        if datetime.fromisoformat(envelope["occurred_at"]) != occurred_at:
            raise ValueError("domain-event envelope occurred_at differs from its Event Store fact")
        try:
            recorded = self._recorded(stream_id, stream_version, event_name, payload, global_position, schema_version)
        except ValueError as error:
            raise CanonicalEventDecodeError(event_id, error) from error
        if recorded.event.event_id != event_id:
            raise ValueError("Event Store payload event_id differs from its Event Store row")
        return CanonicalEvent(recorded, correlation_id, causation_id, actor_id, tenant_id)

    def append(
        self, *, stream_id: str, expected_version: int, events: list[DomainEvent], idempotency_key: str,
        command_name: str, request_hash: str, context: CommandContext,
    ) -> int:
        with self.connection() as connection, connection.transaction(), connection.cursor() as cursor:
            # A key-level advisory lock closes the no-row race before receipt
            # insertion. It serializes identical retries across every stream.
            cursor.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (idempotency_key,))
            cursor.execute(
                "SELECT stream_id, command_name, request_hash, actor_id, tenant_id, resulting_version FROM command_receipt WHERE idempotency_key = %s FOR UPDATE",
                (idempotency_key,),
            )
            receipt = cursor.fetchone()
            if receipt is not None:
                if tuple(receipt[:5]) != (stream_id, command_name, request_hash, context.actor_id or "", context.tenant_id or ""):
                    raise IdempotencyKeyReused("idempotency key was already used for a different command")
                return int(receipt[5])
            if not events:
                raise ValueError("an append requires at least one domain event")
            # A dedicated head row serializes a stream without taking broad table locks or leaking raw unique errors.
            cursor.execute(
                "INSERT INTO event_stream_head(stream_id) VALUES (%s) ON CONFLICT (stream_id) DO NOTHING",
                (stream_id,),
            )
            cursor.execute(
                "SELECT current_version FROM event_stream_head WHERE stream_id = %s FOR UPDATE", (stream_id,)
            )
            head = cursor.fetchone()
            assert head is not None
            actual_version = int(head[0])
            if actual_version != expected_version:
                raise OptimisticConcurrencyConflict(
                    f"{stream_id} advanced to version {actual_version}; command expected {expected_version}"
                )
            version = actual_version
            for event in events:
                if event.event_name == "SettlementRequested":
                    winning_bid_event_id = str(event.payload()["winning_bid_event_id"])
                    cursor.execute(
                        "SELECT event_name, stream_id, payload FROM event_stream WHERE event_id=%s",
                        (winning_bid_event_id,),
                    )
                    cause = cursor.fetchone()
                    expected_auction_stream = f"auction:{event.payload()['auction_id']}"
                    if (
                        cause is None or cause[0] != "WinningBidAccepted" or cause[1] != expected_auction_stream
                        or cause[2].get("company_id") != event.payload()["payer_company_id"]
                        or cause[2].get("accepted_offer") != event.payload()["amount"]
                    ):
                        raise InvalidSettlementCausation("SettlementRequested must cite its Auction's canonical WinningBidAccepted event")
                    cursor.execute(
                        "INSERT INTO settlement_causation_claim(winning_bid_event_id, settlement_stream_id) VALUES (%s, %s) "
                        "ON CONFLICT (winning_bid_event_id) DO NOTHING RETURNING settlement_stream_id",
                        (winning_bid_event_id, stream_id),
                    )
                    claim = cursor.fetchone()
                    if claim is None:
                        cursor.execute(
                            "SELECT settlement_stream_id FROM settlement_causation_claim WHERE winning_bid_event_id=%s",
                            (winning_bid_event_id,),
                        )
                        owner = cursor.fetchone()
                        if owner is None or owner[0] != stream_id:
                            raise SettlementAlreadyRequestedForWinningBid("winning bid already caused another settlement")
                if event.event_name in {"SettlementConfirmed", "LateSettlementDetected"}:
                    provider_reference = str(event.payload()["provider_reference"])
                    cursor.execute(
                        "INSERT INTO provider_reference_claim(provider_reference, settlement_stream_id) VALUES (%s, %s) "
                        "ON CONFLICT (provider_reference) DO NOTHING RETURNING settlement_stream_id",
                        (provider_reference, stream_id),
                    )
                    claim = cursor.fetchone()
                    if claim is None:
                        cursor.execute(
                            "SELECT settlement_stream_id FROM provider_reference_claim WHERE provider_reference=%s",
                            (provider_reference,),
                        )
                        owner = cursor.fetchone()
                        if owner is None or owner[0] != stream_id:
                            raise ProviderReferenceAlreadyObserved("provider reference belongs to another settlement")
                version += 1
                payload = event.payload()
                cursor.execute(
                    "INSERT INTO event_stream(event_id, stream_id, stream_version, event_name, payload, occurred_at, schema_version, correlation_id, causation_id, actor_id, tenant_id) "
                    "VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s) RETURNING global_position",
                    (event.event_id, stream_id, version, event.event_name, json.dumps(payload), event.occurred_at,
                     EVENT_SCHEMA_VERSIONS[event.event_name], context.correlation_id, context.causation_id, context.actor_id, context.tenant_id),
                )
                inserted = cursor.fetchone()
                assert inserted is not None
                global_position = int(inserted[0])
                envelope = {
                    "event_id": str(event.event_id), "event_name": event.event_name,
                    "schema_version": EVENT_SCHEMA_VERSIONS[event.event_name], "stream_id": stream_id,
                    "stream_version": version, "global_position": global_position,
                    "occurred_at": event.occurred_at.isoformat(), "payload": payload,
                    "correlation_id": context.correlation_id, "causation_id": context.causation_id,
                    "actor_id": context.actor_id, "tenant_id": context.tenant_id,
                }
                cursor.execute(
                    "INSERT INTO transactional_outbox(event_id, topic, partition_key, body) "
                    "VALUES (%s, %s, %s, %s::jsonb)",
                    (event.event_id, "thyphon.domain-events", stream_id, json.dumps(envelope)),
                )
            cursor.execute(
                "UPDATE event_stream_head SET current_version = %s WHERE stream_id = %s",
                (version, stream_id),
            )
            cursor.execute(
                "INSERT INTO command_receipt(idempotency_key, stream_id, command_name, request_hash, actor_id, tenant_id, resulting_version, accepted_at) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                (idempotency_key, stream_id, command_name, request_hash, context.actor_id or "", context.tenant_id or "", version, datetime.now(UTC)),
            )
            return version

    @staticmethod
    def _recorded(stream_id: str, version: int, event_name: str, payload: dict[str, Any], global_position: int, schema_version: int) -> RecordedEvent:
        event_type = EVENT_TYPES[event_name]
        schema_version, payload = upcast_event(event_name, int(schema_version), payload)
        event = event_type(**{key: _decode_value(key, value) for key, value in payload.items()})
        return RecordedEvent(stream_id, int(version), event, int(global_position), schema_version)
