from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from thyphon.infrastructure.sqlite_event_store import EVENT_SCHEMA_VERSIONS, EVENT_TYPES, _decode_value, upcast_event
from thyphon.shared.domain import CommandContext, DomainEvent, IdempotencyKeyReused, OptimisticConcurrencyConflict, ProviderReferenceAlreadyObserved, RecordedEvent


class PostgresEventStore:
    """Production adapter: event append and transactional outbox share one PostgreSQL transaction.

    `psycopg` is intentionally imported only at construction so domain/Bazel tests remain hermetic.
    """

    def __init__(self, connection_string: str) -> None:
        try:
            import psycopg
        except ImportError as error:  # pragma: no cover - exercised by runtime setup
            raise RuntimeError("Install Thyphon with the 'runtime' extra for PostgreSQL support") from error
        # Read-side lookups must not accidentally hold an outer transaction open.
        # Atomic appends still use the explicit `connection.transaction()` block below.
        self.connection = psycopg.connect(connection_string, autocommit=True)

    def idempotency_result(self, idempotency_key: str, *, stream_id: str, command_name: str, request_hash: str) -> int | None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                "SELECT stream_id, command_name, request_hash, resulting_version FROM command_receipt WHERE idempotency_key = %s", (idempotency_key,)
            )
            row = cursor.fetchone()
        if row is None:
            return None
        if tuple(row[:3]) != (stream_id, command_name, request_hash):
            raise IdempotencyKeyReused("idempotency key was already used for a different command")
        return int(row[3])

    def read_stream(self, stream_id: str) -> list[RecordedEvent]:
        with self.connection.cursor() as cursor:
            cursor.execute(
                "SELECT stream_id, stream_version, event_name, payload, global_position, schema_version FROM event_stream "
                "WHERE stream_id = %s ORDER BY stream_version", (stream_id,)
            )
            rows = cursor.fetchall()
        return [self._recorded(*row) for row in rows]

    def append(
        self, *, stream_id: str, expected_version: int, events: list[DomainEvent], idempotency_key: str,
        command_name: str, request_hash: str, context: CommandContext,
    ) -> int:
        with self.connection.transaction(), self.connection.cursor() as cursor:
            cursor.execute(
                "SELECT stream_id, command_name, request_hash, resulting_version FROM command_receipt WHERE idempotency_key = %s FOR UPDATE",
                (idempotency_key,),
            )
            receipt = cursor.fetchone()
            if receipt is not None:
                if tuple(receipt[:3]) != (stream_id, command_name, request_hash):
                    raise IdempotencyKeyReused("idempotency key was already used for a different command")
                return int(receipt[3])
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
                "INSERT INTO command_receipt(idempotency_key, stream_id, command_name, request_hash, resulting_version, accepted_at) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                (idempotency_key, stream_id, command_name, request_hash, version, datetime.now(UTC)),
            )
            return version

    @staticmethod
    def _recorded(stream_id: str, version: int, event_name: str, payload: dict[str, Any], global_position: int, schema_version: int) -> RecordedEvent:
        event_type = EVENT_TYPES[event_name]
        schema_version, payload = upcast_event(event_name, int(schema_version), payload)
        event = event_type(**{key: _decode_value(key, value) for key, value in payload.items()})
        return RecordedEvent(stream_id, int(version), event, int(global_position), schema_version)
