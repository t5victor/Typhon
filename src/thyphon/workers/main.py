from __future__ import annotations

import asyncio
import json
import os
import sys
from typing import Any

from thyphon.projections.postgres_auction_overview import PostgresAuctionOverviewProjector


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
            with connection.transaction(), connection.cursor() as cursor:
                cursor.execute(
                    "SELECT o.event_id, o.topic, o.partition_key, o.body FROM transactional_outbox o "
                    "JOIN event_stream e ON e.event_id = o.event_id "
                    "WHERE o.published_at IS NULL "
                    "ORDER BY e.stream_id, e.stream_version "
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
            await asyncio.sleep(0.1)
    finally:
        await producer.stop()
        connection.close()


async def run_projection() -> None:
    _, consumer_type, _ = _runtime()
    bootstrap = os.environ["THYPHON_KAFKA_BOOTSTRAP"]
    dsn = os.environ["THYPHON_DATABASE_URL"]
    projector = PostgresAuctionOverviewProjector(dsn)
    consumer = consumer_type(
        "thyphon.domain-events", bootstrap_servers=bootstrap, group_id="thyphon-auction-overview-v1",
        enable_auto_commit=False, auto_offset_reset="earliest",
    )
    await consumer.start()
    try:
        async for message in consumer:
            envelope: dict[str, Any] = json.loads(message.value)
            projector.apply(projector.decode(envelope))
            await consumer.commit()
    finally:
        await consumer.stop()


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
