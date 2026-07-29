from __future__ import annotations

import asyncio
import base64
import json
import os
import sys
from contextlib import contextmanager
from typing import Any
from uuid import UUID

from thyphon.application.settlement_commands import SettlementCommandHandler
from thyphon.projections.postgres_auction_overview import PostgresAuctionOverviewProjector
from thyphon.infrastructure.postgres_event_store import PostgresEventStore
from thyphon.auction.domain.events.winning_bid_accepted.event import WinningBidAccepted
from thyphon.settlement.domain.commands.request_settlement.command import RequestSettlement
from thyphon.shared.domain import CommandContext, aggregate_id


def _runtime():
    try:
        import psycopg
        from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
        return psycopg, AIOKafkaConsumer, AIOKafkaProducer
    except ImportError as error:  # pragma: no cover - runtime only
        raise RuntimeError("Install Thyphon with the 'runtime' extra") from error


async def run_outbox() -> None:
    psycopg, _, producer_type = _runtime()
    dsn = os.environ["THYPHON_DATABASE_URL"]
    bootstrap = os.environ["THYPHON_KAFKA_BOOTSTRAP"]
    producer = producer_type(bootstrap_servers=bootstrap, acks="all")
    await producer.start()
    connection = psycopg.connect(dsn, row_factory=psycopg.rows.dict_row, autocommit=True)
    try:
        while True:
            try:
                with connection.transaction(), connection.cursor() as cursor:
                    cursor.execute(
                        "SELECT o.event_id, o.topic, o.partition_key, o.body FROM transactional_outbox o "
                        "JOIN event_stream e ON e.event_id = o.event_id "
                        "WHERE o.published_at IS NULL "
                        "ORDER BY e.global_position "
                        "LIMIT 50 FOR UPDATE OF o SKIP LOCKED"
                    )
                    pending = cursor.fetchall()
                    for row in pending:
                        await producer.send_and_wait(
                            row["topic"], key=row["partition_key"].encode(),
                            value=json.dumps(row["body"], sort_keys=True).encode(),
                        )
                        cursor.execute(
                            "UPDATE transactional_outbox SET published_at=NOW() WHERE event_id=%s", (row["event_id"],)
                        )
            except Exception:
                # Publication is at-least-once: the transaction rolls back and
                # the same immutable outbox record is retried after backoff.
                await asyncio.sleep(1.0)
                continue
            await asyncio.sleep(0.1)
    finally:
        await producer.stop()
        connection.close()


async def run_redrive_outbox() -> None:
    """Publish durable redrive intents after their attempt record is committed."""
    psycopg, _, producer_type = _runtime()
    dsn = os.environ["THYPHON_DATABASE_URL"]
    bootstrap = os.environ["THYPHON_KAFKA_BOOTSTRAP"]
    producer = producer_type(bootstrap_servers=bootstrap, acks="all")
    await producer.start()
    connection = psycopg.connect(dsn, row_factory=psycopg.rows.dict_row, autocommit=True)
    try:
        while True:
            try:
                with connection.transaction(), connection.cursor() as cursor:
                    cursor.execute(
                        "SELECT attempt_id, envelope FROM projection_redrive_attempt WHERE published_at IS NULL "
                        "ORDER BY requested_at LIMIT 50 FOR UPDATE SKIP LOCKED"
                    )
                    pending = cursor.fetchall()
                    for attempt in pending:
                        envelope = attempt["envelope"]
                        await producer.send_and_wait(
                            "thyphon.domain-events", key=envelope["stream_id"].encode(),
                            value=json.dumps(envelope, sort_keys=True).encode(),
                            headers=[("thyphon-redrive-attempt", str(attempt["attempt_id"]).encode())],
                        )
                        cursor.execute(
                            "UPDATE projection_redrive_attempt SET published_at=NOW() WHERE attempt_id=%s",
                            (attempt["attempt_id"],),
                        )
            except Exception:
                await asyncio.sleep(1.0)
                continue
            await asyncio.sleep(0.1)
    finally:
        await producer.stop()
        connection.close()


def _parse_envelope(raw_value: bytes | None) -> dict[str, Any]:
    if raw_value is None:
        raise ValueError("Kafka delivered a tombstone on the domain-events topic")
    decoded = json.loads(raw_value)
    if not isinstance(decoded, dict):
        raise ValueError("Kafka domain-event record must be a JSON object")
    return decoded


def _redrive_attempt_id(headers: list[tuple[str, bytes]] | None) -> UUID | None:
    for name, value in headers or []:
        if name == "thyphon-redrive-attempt":
            try:
                return UUID(value.decode())
            except (AttributeError, UnicodeDecodeError, ValueError) as error:
                raise ValueError("Kafka redrive attempt header is invalid") from error
    return None


def _event_id_or_none(envelope: dict[str, Any] | None) -> UUID | None:
    if envelope is None:
        return None
    try:
        return UUID(str(envelope.get("event_id")))
    except (AttributeError, TypeError, ValueError):
        return None


def _is_infrastructure_error(error: Exception) -> bool:
    """Failures that must leave the Kafka offset uncommitted for retry."""
    return isinstance(error, (TimeoutError, ConnectionError, OSError)) or error.__class__.__name__ in {
        "OperationalError", "InterfaceError", "PoolTimeout", "ConnectionTimeout",
    }


def _redrive_delivery_requires_rebuild(*, attempt_resolved_at: Any, failure_resolved_at: Any) -> bool:
    """Classify a verified redrive delivery while its state rows are locked.

    Kafka may redeliver an attempt after the first delivery has completed.  That
    is a successful no-op, whereas an unresolved attempt with a resolved
    failure is an inconsistent control-plane state and must not rebuild.
    """
    if attempt_resolved_at is not None:
        return False
    if failure_resolved_at is not None:
        raise ValueError("Kafka redrive attempt is inactive because its failure is already resolved")
    return True


@contextmanager
def _locked_active_redrive_attempt(
    failure_store: Any, *, attempt_id: UUID, consumer_name: str, event_id: UUID,
):
    """Lock and classify the exact redrive attempt allowed to repair an aggregate."""
    with failure_store.transaction(), failure_store.cursor() as cursor:
        cursor.execute(
            "SELECT a.resolved_at AS attempt_resolved_at, f.resolved_at AS failure_resolved_at "
            "FROM projection_redrive_attempt a "
            "JOIN projection_failure f ON f.active_redrive_attempt_id=a.attempt_id "
            "WHERE a.attempt_id=%s AND a.consumer_name=%s AND a.event_id=%s "
            "AND f.consumer_name=%s AND f.event_id=%s "
            "FOR UPDATE OF a, f",
            (attempt_id, consumer_name, event_id, consumer_name, event_id),
        )
        attempt = cursor.fetchone()
        if attempt is None:
            raise ValueError("Kafka redrive attempt is unknown or belongs to another event")
        requires_rebuild = _redrive_delivery_requires_rebuild(
            attempt_resolved_at=attempt[0],
            failure_resolved_at=attempt[1],
        )
        yield requires_rebuild
        if not requires_rebuild:
            return
        cursor.execute(
            "UPDATE projection_redrive_attempt SET resolved_at=NOW() "
            "WHERE attempt_id=%s AND consumer_name=%s AND event_id=%s AND resolved_at IS NULL",
            (attempt_id, consumer_name, event_id),
        )
        cursor.execute(
            "UPDATE projection_failure SET resolved_at=NOW() "
            "WHERE consumer_name=%s AND event_id=%s AND active_redrive_attempt_id=%s AND resolved_at IS NULL",
            (consumer_name, event_id, attempt_id),
        )


async def _quarantine(
    *, message: Any, envelope: dict[str, Any] | None, error: Exception, event_id: UUID | None,
    candidate_event_id: UUID | None,
    failure_store: Any, producer: Any, consumer_name: str,
) -> None:
    """Quarantine every malformed record, even when it has no usable event_id."""
    raw_value = message.value
    if event_id is None:
        with failure_store.transaction(), failure_store.cursor() as cursor:
            cursor.execute(
                "INSERT INTO projection_raw_failure(consumer_name, topic, partition_id, message_offset, raw_value, attempts, last_error, quarantined_at) "
                "VALUES (%s, %s, %s, %s, %s, 1, %s, NOW()) "
                "ON CONFLICT (consumer_name, topic, partition_id, message_offset) DO UPDATE "
                "SET attempts=projection_raw_failure.attempts+1, last_error=EXCLUDED.last_error, quarantined_at=NOW()",
                (consumer_name, message.topic, message.partition, message.offset, raw_value, str(error)[:1000]),
            )
    else:
        with failure_store.transaction(), failure_store.cursor() as cursor:
            cursor.execute(
                "INSERT INTO projection_failure(consumer_name, event_id, attempts, last_error, quarantined_at) "
                "VALUES (%s, %s, 3, %s, NOW()) "
                "ON CONFLICT (consumer_name, event_id) DO UPDATE "
                "SET attempts=projection_failure.attempts+1, last_error=EXCLUDED.last_error, quarantined_at=NOW()",
                (consumer_name, event_id, str(error)[:1000]),
            )
    dead_letter = {
        "consumer": consumer_name, "error": str(error), "topic": message.topic,
        "partition": message.partition, "offset": message.offset, "envelope": envelope,
        "candidate_event_id": None if candidate_event_id is None else str(candidate_event_id),
        "raw_value_base64": None if raw_value is None else base64.b64encode(raw_value).decode(),
    }
    key = str(event_id or f"{message.topic}:{message.partition}:{message.offset}").encode()
    await producer.send_and_wait(
        "thyphon.domain-events-dlq", key=key, value=json.dumps(dead_letter, sort_keys=True).encode(),
    )


def _request_settlement_for_winning_bid(settlements: SettlementCommandHandler, canonical: Any) -> None:
    recorded = canonical.recorded
    if not isinstance(recorded.event, WinningBidAccepted):
        return
    if not recorded.stream_id.startswith("auction:"):
        raise ValueError("WinningBidAccepted must belong to an auction stream")
    settlements.request_settlement(RequestSettlement(
        settlement_id=f"settlement-{aggregate_id(recorded.stream_id, 'auction')}",
        auction_id=aggregate_id(recorded.stream_id, "auction"),
        payer_company_id=recorded.event.company_id,
        amount=recorded.event.accepted_offer,
        winning_bid_event_id=recorded.event.event_id,
    ), CommandContext(
        idempotency_key=f"winning-bid:{recorded.event.event_id}",
        correlation_id=canonical.correlation_id, causation_id=str(recorded.event.event_id),
        actor_id="settlement-process-manager", tenant_id=canonical.tenant_id,
    ))


async def run_projection() -> None:
    psycopg, consumer_type, producer_type = _runtime()
    bootstrap = os.environ["THYPHON_KAFKA_BOOTSTRAP"]
    dsn = os.environ["THYPHON_DATABASE_URL"]
    projector = PostgresAuctionOverviewProjector(dsn)
    settlement_store = PostgresEventStore(dsn)
    settlements = SettlementCommandHandler(settlement_store)
    failure_store = psycopg.connect(dsn, autocommit=True)
    dlq_producer = producer_type(bootstrap_servers=bootstrap, acks="all")
    consumer = consumer_type(
        "thyphon.domain-events", bootstrap_servers=bootstrap, group_id="thyphon-auction-overview-v1",
        enable_auto_commit=False, auto_offset_reset="earliest",
    )
    await consumer.start()
    await dlq_producer.start()
    try:
        infrastructure_retries: dict[tuple[str, int, int], int] = {}
        async for message in consumer:
            envelope: dict[str, Any] | None = None
            candidate_event_id: UUID | None = None
            canonical_event_id: UUID | None = None
            failure: Exception | None = None
            infrastructure_error: Exception | None = None
            redrive_attempt: UUID | None = None
            try:
                envelope = _parse_envelope(message.value)
                candidate_event_id = _event_id_or_none(envelope)
                redrive_attempt = _redrive_attempt_id(message.headers)
                for attempt in range(1, 4):
                    try:
                        # The broker supplies delivery only. This lookup rejects
                        # forged, altered and payload/metadata-divergent records.
                        canonical = settlement_store.canonical_event(envelope)
                        recorded = canonical.recorded
                        canonical_event_id = recorded.event.event_id
                        if redrive_attempt is not None and recorded.stream_id.startswith("auction:"):
                            with _locked_active_redrive_attempt(
                                failure_store, attempt_id=redrive_attempt, consumer_name=projector.consumer_name,
                                event_id=canonical_event_id,
                            ) as requires_rebuild:
                                if not requires_rebuild:
                                    failure = None
                                    break
                                projector.rebuild_stream(recorded.stream_id)
                                _request_settlement_for_winning_bid(settlements, canonical)
                        else:
                            projector.apply(recorded)
                            _request_settlement_for_winning_bid(settlements, canonical)
                        failure = None
                        break
                    except Exception as error:
                        if _is_infrastructure_error(error):
                            infrastructure_error = error
                            break
                        failure = error
                        if attempt < 3:
                            await asyncio.sleep(0.1 * attempt)
            except Exception as error:
                if _is_infrastructure_error(error):
                    infrastructure_error = error
                else:
                    failure = error
            if infrastructure_error is not None:
                retry_key = (message.topic, message.partition, message.offset)
                retry_count = infrastructure_retries.get(retry_key, 0) + 1
                infrastructure_retries[retry_key] = retry_count
                await asyncio.sleep(min(30.0, 0.25 * (2 ** (retry_count - 1))))
                from aiokafka import TopicPartition
                consumer.seek(TopicPartition(message.topic, message.partition), message.offset)
                continue
            infrastructure_retries.pop((message.topic, message.partition, message.offset), None)
            if failure is not None:
                await _quarantine(
                    message=message, envelope=envelope, error=failure, event_id=canonical_event_id,
                    candidate_event_id=candidate_event_id,
                    failure_store=failure_store, producer=dlq_producer, consumer_name=projector.consumer_name,
                )
            with failure_store.transaction(), failure_store.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO process_checkpoint(process_name, last_observed_at) VALUES (%s, NOW()) "
                    "ON CONFLICT (process_name) DO UPDATE SET last_observed_at=EXCLUDED.last_observed_at",
                    (projector.consumer_name,),
                )
            await consumer.commit()
    finally:
        await consumer.stop()
        await dlq_producer.stop()
        failure_store.close()
        settlement_store.close()
        projector.close()


def main() -> None:
    mode = sys.argv[1] if len(sys.argv) == 2 else ""
    if mode == "outbox":
        asyncio.run(run_outbox())
    elif mode == "redrive-outbox":
        asyncio.run(run_redrive_outbox())
    elif mode == "projection":
        asyncio.run(run_projection())
    else:
        raise SystemExit("usage: python -m thyphon.workers.main {outbox|redrive-outbox|projection}")


if __name__ == "__main__":
    main()
