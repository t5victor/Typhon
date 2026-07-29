from __future__ import annotations

import asyncio
import json
import os
import sys
from typing import Any

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
        async for message in consumer:
            if message.value is None:
                raise ValueError("Kafka delivered an empty domain-event record")
            envelope: dict[str, Any] = json.loads(message.value)
            failure: Exception | None = None
            for attempt in range(1, 4):
                try:
                    recorded = projector.decode(envelope)
                    projector.apply(recorded)
                    if isinstance(recorded.event, WinningBidAccepted):
                        # Settlement is derived from a fact already accepted by Auction, never from client supplied money.
                        settlements.request_settlement(RequestSettlement(
                            settlement_id=f"settlement-{aggregate_id(recorded.stream_id, 'auction')}",
                            auction_id=aggregate_id(recorded.stream_id, "auction"),
                            payer_company_id=recorded.event.company_id,
                            amount=recorded.event.accepted_offer,
                            winning_bid_event_id=str(recorded.event.event_id),
                        ), CommandContext(
                            idempotency_key=f"winning-bid:{recorded.event.event_id}",
                            correlation_id=envelope["correlation_id"], causation_id=envelope["event_id"],
                            actor_id="settlement-process-manager", tenant_id=envelope.get("tenant_id"),
                        ))
                    failure = None
                    break
                except Exception as error:  # a poison event is quarantined after bounded retries
                    failure = error
                    with failure_store.transaction(), failure_store.cursor() as cursor:
                        cursor.execute(
                            "INSERT INTO projection_failure(consumer_name, event_id, attempts, last_error) "
                            "VALUES (%s, %s, 1, %s) "
                            "ON CONFLICT (consumer_name, event_id) DO UPDATE "
                            "SET attempts=projection_failure.attempts+1, last_error=EXCLUDED.last_error "
                            "RETURNING attempts",
                            (projector.consumer_name, envelope["event_id"], str(error)[:1000]),
                        )
                        cursor.fetchone()
                    if attempt < 3:
                        await asyncio.sleep(0.1 * attempt)
            if failure is not None:
                dead_letter = {"envelope": envelope, "consumer": projector.consumer_name, "error": str(failure)}
                await dlq_producer.send_and_wait(
                    "thyphon.domain-events-dlq", key=envelope["stream_id"].encode(),
                    value=json.dumps(dead_letter, sort_keys=True).encode(),
                )
                with failure_store.transaction(), failure_store.cursor() as cursor:
                    cursor.execute(
                        "UPDATE projection_failure SET quarantined_at=NOW() WHERE consumer_name=%s AND event_id=%s",
                        (projector.consumer_name, envelope["event_id"]),
                    )
            else:
                # Redrive only records an intent to retry.  The event is
                # resolved once this consumer has actually completed it.
                with failure_store.transaction(), failure_store.cursor() as cursor:
                    cursor.execute(
                        "UPDATE projection_failure SET resolved_at=NOW() "
                        "WHERE consumer_name=%s AND event_id=%s "
                        "AND redriven_at IS NOT NULL AND resolved_at IS NULL",
                        (projector.consumer_name, envelope["event_id"]),
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
    elif mode == "projection":
        asyncio.run(run_projection())
    else:
        raise SystemExit("usage: python -m thyphon.workers.main {outbox|projection}")


if __name__ == "__main__":
    main()
