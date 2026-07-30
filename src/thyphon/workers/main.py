from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import sys
from contextlib import contextmanager
from typing import Any
from uuid import UUID, uuid4

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


def _close_quietly(connection: Any | None) -> None:
    if connection is None:
        return
    try:
        connection.close()
    except Exception:
        pass


async def run_outbox() -> None:
    """Publish canonical events in global order from a reconnectable dispatcher."""
    psycopg, _, producer_type = _runtime()
    dsn = os.environ["THYPHON_DATABASE_URL"]
    bootstrap = os.environ["THYPHON_KAFKA_BOOTSTRAP"]
    producer = producer_type(bootstrap_servers=bootstrap, acks="all")
    await producer.start()
    connection: Any | None = None
    try:
        while True:
            try:
                if connection is None:
                    connection = psycopg.connect(dsn, row_factory=psycopg.rows.dict_row, autocommit=True)
                with connection.transaction(), connection.cursor() as cursor:
                    # One logical dispatcher prevents SKIP LOCKED replicas from
                    # publishing N+1 before N, including within one stream.
                    cursor.execute("SELECT pg_try_advisory_xact_lock(421339) AS locked")
                    lock = cursor.fetchone()
                    if lock is None or not lock["locked"]:
                        pending: list[dict[str, Any]] = []
                    else:
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
            except Exception as error:
                # A restart can invalidate an otherwise open psycopg connection.
                # Discard it rather than leaving a healthy-looking worker wedged.
                _close_quietly(connection)
                connection = None
                print(f"outbox dispatcher reconnecting after {type(error).__name__}: {error}", file=sys.stderr)
                await asyncio.sleep(1.0)
                continue
            await asyncio.sleep(0.1)
    finally:
        await producer.stop()
        _close_quietly(connection)


async def run_redrive_outbox() -> None:
    """Publish durable redrive intents after their attempt record is committed."""
    psycopg, _, producer_type = _runtime()
    dsn = os.environ["THYPHON_DATABASE_URL"]
    bootstrap = os.environ["THYPHON_KAFKA_BOOTSTRAP"]
    producer = producer_type(bootstrap_servers=bootstrap, acks="all")
    await producer.start()
    connection: Any | None = None
    try:
        while True:
            try:
                if connection is None:
                    connection = psycopg.connect(dsn, row_factory=psycopg.rows.dict_row, autocommit=True)
                with connection.transaction(), connection.cursor() as cursor:
                    cursor.execute(
                        "SELECT attempt_id, envelope FROM projection_redrive_attempt WHERE status='pending' "
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
                            "UPDATE projection_redrive_attempt SET published_at=NOW(), status='published' "
                            "WHERE attempt_id=%s AND status='pending'",
                            (attempt["attempt_id"],),
                        )
            except Exception as error:
                _close_quietly(connection)
                connection = None
                print(f"redrive dispatcher reconnecting after {type(error).__name__}: {error}", file=sys.stderr)
                await asyncio.sleep(1.0)
                continue
            await asyncio.sleep(0.1)
    finally:
        await producer.stop()
        _close_quietly(connection)


async def run_dead_letter_outbox() -> None:
    """Publish bounded dead-letter references after quarantine is durable."""
    psycopg, _, producer_type = _runtime()
    dsn = os.environ["THYPHON_DATABASE_URL"]
    bootstrap = os.environ["THYPHON_KAFKA_BOOTSTRAP"]
    producer = producer_type(bootstrap_servers=bootstrap, acks="all")
    await producer.start()
    connection: Any | None = None
    try:
        while True:
            try:
                if connection is None:
                    connection = psycopg.connect(dsn, row_factory=psycopg.rows.dict_row, autocommit=True)
                with connection.transaction(), connection.cursor() as cursor:
                    cursor.execute(
                        "SELECT dead_letter_id, consumer_name, source_topic, partition_id, message_offset, "
                        "canonical_event_id, candidate_event_id, raw_sha256, raw_size, preview_base64, last_error "
                        "FROM projection_dead_letter_outbox WHERE published_at IS NULL "
                        "ORDER BY created_at LIMIT 50 FOR UPDATE SKIP LOCKED"
                    )
                    for row in cursor.fetchall():
                        payload = {
                            "dead_letter_id": str(row["dead_letter_id"]), "consumer": row["consumer_name"],
                            "source": {"topic": row["source_topic"], "partition": row["partition_id"], "offset": row["message_offset"]},
                            "canonical_event_id": None if row["canonical_event_id"] is None else str(row["canonical_event_id"]),
                            "candidate_event_id": None if row["candidate_event_id"] is None else str(row["candidate_event_id"]),
                            "raw_sha256": row["raw_sha256"], "raw_size": row["raw_size"],
                            "preview_base64": row["preview_base64"], "error": row["last_error"],
                        }
                        await producer.send_and_wait(
                            "thyphon.domain-events-dlq", key=str(row["dead_letter_id"]).encode(),
                            value=json.dumps(payload, sort_keys=True).encode(),
                        )
                        cursor.execute(
                            "UPDATE projection_dead_letter_outbox SET published_at=NOW() WHERE dead_letter_id=%s",
                            (row["dead_letter_id"],),
                        )
            except Exception as error:
                _close_quietly(connection)
                connection = None
                print(f"dead-letter dispatcher reconnecting after {type(error).__name__}: {error}", file=sys.stderr)
                await asyncio.sleep(1.0)
                continue
            await asyncio.sleep(0.1)
    finally:
        await producer.stop()
        _close_quietly(connection)


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


def _redrive_delivery_requires_rebuild(
    *, attempt_status: str, failure_resolved_at: Any, active_attempt_id: Any, attempt_id: UUID,
) -> bool:
    """Classify a verified redrive delivery while its state rows are locked.

    A pending attempt can legitimately arrive before the dispatcher persists
    ``published_at``. Kafka may also redeliver completed or failed attempts;
    both are successful no-ops. Only the one active pending/published attempt
    is allowed to repair an unresolved failure.
    """
    if attempt_status in {"resolved", "failed", "superseded"}:
        return False
    if attempt_status not in {"pending", "published"}:
        raise ValueError("Kafka redrive attempt has an unknown lifecycle state")
    if failure_resolved_at is not None:
        raise ValueError("Kafka redrive attempt is inactive because its failure is already resolved")
    if str(active_attempt_id) != str(attempt_id):
        raise ValueError("Kafka redrive attempt is no longer active for this failure")
    return True


@contextmanager
def _locked_active_redrive_attempt(
    failure_store: Any, *, attempt_id: UUID, consumer_name: str, event_id: UUID,
):
    """Lock and classify the exact redrive attempt allowed to repair an aggregate."""
    with failure_store.transaction(), failure_store.cursor() as cursor:
        cursor.execute(
            "SELECT a.status, f.resolved_at AS failure_resolved_at, f.active_redrive_attempt_id "
            "FROM projection_redrive_attempt a "
            "JOIN projection_failure f ON f.consumer_name=a.consumer_name AND f.event_id=a.event_id "
            "WHERE a.attempt_id=%s AND a.consumer_name=%s AND a.event_id=%s "
            "FOR UPDATE OF a, f",
            (attempt_id, consumer_name, event_id),
        )
        attempt = cursor.fetchone()
        if attempt is None:
            raise ValueError("Kafka redrive attempt is unknown or belongs to another event")
        requires_rebuild = _redrive_delivery_requires_rebuild(
            attempt_status=attempt[0],
            failure_resolved_at=attempt[1],
            active_attempt_id=attempt[2],
            attempt_id=attempt_id,
        )
        yield requires_rebuild
        if not requires_rebuild:
            return
        cursor.execute(
            "UPDATE projection_redrive_attempt SET resolved_at=NOW(), status='resolved', last_error=NULL "
            "WHERE attempt_id=%s AND consumer_name=%s AND event_id=%s AND status IN ('pending', 'published')",
            (attempt_id, consumer_name, event_id),
        )
        cursor.execute(
            "UPDATE projection_failure SET resolved_at=NOW() "
            "WHERE consumer_name=%s AND event_id=%s AND active_redrive_attempt_id=%s AND resolved_at IS NULL",
            (consumer_name, event_id, attempt_id),
        )


def _dead_letter_preview(raw_value: bytes | None, limit: int = 3072) -> str | None:
    if raw_value is None:
        return None
    return base64.b64encode(raw_value[:limit]).decode()


def _quarantine(
    *, message: Any, error: Exception, event_id: UUID | None,
    candidate_event_id: UUID | None,
    redrive_attempt: UUID | None, failure_store: Any, consumer_name: str,
) -> None:
    """Persist quarantine and a bounded DLQ publication intent atomically."""
    raw_value = message.value
    raw_sha256 = hashlib.sha256(raw_value or b"").hexdigest()
    with failure_store.transaction(), failure_store.cursor() as cursor:
        if event_id is None:
            cursor.execute(
                "INSERT INTO projection_raw_failure(consumer_name, topic, partition_id, message_offset, raw_value, attempts, last_error, quarantined_at) "
                "VALUES (%s, %s, %s, %s, %s, 1, %s, NOW()) "
                "ON CONFLICT (consumer_name, topic, partition_id, message_offset) DO UPDATE "
                "SET attempts=projection_raw_failure.attempts+1, last_error=EXCLUDED.last_error, quarantined_at=NOW()",
                (consumer_name, message.topic, message.partition, message.offset, raw_value, str(error)[:1000]),
            )
        else:
            cursor.execute(
                "INSERT INTO projection_failure(consumer_name, event_id, attempts, last_error, quarantined_at) "
                "VALUES (%s, %s, 3, %s, NOW()) "
                "ON CONFLICT (consumer_name, event_id) DO UPDATE "
                "SET attempts=projection_failure.attempts+1, last_error=EXCLUDED.last_error, quarantined_at=NOW()",
                (consumer_name, event_id, str(error)[:1000]),
            )
        cursor.execute(
            "INSERT INTO projection_dead_letter_outbox("
            "dead_letter_id, consumer_name, source_topic, partition_id, message_offset, canonical_event_id, "
            "candidate_event_id, raw_sha256, raw_size, preview_base64, last_error) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (consumer_name, source_topic, partition_id, message_offset) DO UPDATE "
            "SET canonical_event_id=EXCLUDED.canonical_event_id, candidate_event_id=EXCLUDED.candidate_event_id, "
            "raw_sha256=EXCLUDED.raw_sha256, raw_size=EXCLUDED.raw_size, preview_base64=EXCLUDED.preview_base64, "
            "last_error=EXCLUDED.last_error",
            (
                uuid4(), consumer_name, message.topic, message.partition, message.offset, event_id, candidate_event_id,
                raw_sha256, len(raw_value or b""), _dead_letter_preview(raw_value), str(error)[:1000],
            ),
        )
        if redrive_attempt is not None and event_id is not None:
            cursor.execute(
                "UPDATE projection_redrive_attempt SET status='failed', last_error=%s "
                "WHERE attempt_id=%s AND consumer_name=%s AND event_id=%s AND status IN ('pending', 'published')",
                (str(error)[:1000], redrive_attempt, consumer_name, event_id),
            )
            cursor.execute(
                "UPDATE projection_failure SET active_redrive_attempt_id=NULL "
                "WHERE consumer_name=%s AND event_id=%s AND active_redrive_attempt_id=%s AND resolved_at IS NULL",
                (consumer_name, event_id, redrive_attempt),
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
    _, consumer_type, _ = _runtime()
    try:
        from psycopg_pool import ConnectionPool
    except ImportError as error:  # pragma: no cover - runtime extra
        raise RuntimeError("Install Thyphon with the 'runtime' extra") from error
    bootstrap = os.environ["THYPHON_KAFKA_BOOTSTRAP"]
    dsn = os.environ["THYPHON_DATABASE_URL"]
    projector = PostgresAuctionOverviewProjector(dsn)
    settlement_store = PostgresEventStore(dsn)
    settlements = SettlementCommandHandler(settlement_store)
    failure_pool: Any = ConnectionPool(conninfo=dsn, min_size=1, max_size=4, kwargs={"autocommit": True}, open=True)
    consumer = consumer_type(
        "thyphon.domain-events", bootstrap_servers=bootstrap, group_id="thyphon-auction-overview-v1",
        enable_auto_commit=False, auto_offset_reset="earliest",
    )
    await consumer.start()
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
                            with failure_pool.connection() as failure_store:
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
                try:
                    with failure_pool.connection() as failure_store:
                        _quarantine(
                            message=message, error=failure, event_id=canonical_event_id,
                            candidate_event_id=candidate_event_id,
                            redrive_attempt=redrive_attempt, failure_store=failure_store,
                            consumer_name=projector.consumer_name,
                        )
                except Exception as quarantine_error:
                    if _is_infrastructure_error(quarantine_error):
                        retry_key = (message.topic, message.partition, message.offset)
                        retry_count = infrastructure_retries.get(retry_key, 0) + 1
                        infrastructure_retries[retry_key] = retry_count
                        await asyncio.sleep(min(30.0, 0.25 * (2 ** (retry_count - 1))))
                        from aiokafka import TopicPartition
                        consumer.seek(TopicPartition(message.topic, message.partition), message.offset)
                        continue
                    raise
            try:
                with failure_pool.connection() as failure_store:
                    with failure_store.transaction(), failure_store.cursor() as cursor:
                        cursor.execute(
                            "INSERT INTO process_checkpoint(process_name, last_observed_at) VALUES (%s, NOW()) "
                            "ON CONFLICT (process_name) DO UPDATE SET last_observed_at=EXCLUDED.last_observed_at",
                            (projector.consumer_name,),
                        )
            except Exception as checkpoint_error:
                if _is_infrastructure_error(checkpoint_error):
                    retry_key = (message.topic, message.partition, message.offset)
                    retry_count = infrastructure_retries.get(retry_key, 0) + 1
                    infrastructure_retries[retry_key] = retry_count
                    await asyncio.sleep(min(30.0, 0.25 * (2 ** (retry_count - 1))))
                    from aiokafka import TopicPartition
                    consumer.seek(TopicPartition(message.topic, message.partition), message.offset)
                    continue
                raise
            await consumer.commit()
    finally:
        await consumer.stop()
        failure_pool.close()
        settlement_store.close()
        projector.close()


def main() -> None:
    mode = sys.argv[1] if len(sys.argv) == 2 else ""
    if mode == "outbox":
        asyncio.run(run_outbox())
    elif mode == "redrive-outbox":
        asyncio.run(run_redrive_outbox())
    elif mode == "dead-letter-outbox":
        asyncio.run(run_dead_letter_outbox())
    elif mode == "projection":
        asyncio.run(run_projection())
    else:
        raise SystemExit("usage: python -m thyphon.workers.main {outbox|redrive-outbox|dead-letter-outbox|projection}")


if __name__ == "__main__":
    main()
