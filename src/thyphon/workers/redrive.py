"""Manual DLQ redrive for one quarantined event.

The action only marks that the event was re-enqueued.  The projection worker
marks it resolved after the normal idempotent processing path succeeds.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from uuid import UUID


async def redrive(event_id: UUID) -> None:
    import psycopg
    from aiokafka import AIOKafkaProducer

    dsn = os.environ["THYPHON_DATABASE_URL"]
    bootstrap = os.environ["THYPHON_KAFKA_BOOTSTRAP"]
    connection = psycopg.connect(dsn, row_factory=psycopg.rows.dict_row, autocommit=True)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT e.event_id, e.event_name, e.schema_version, e.stream_id, e.stream_version, e.global_position, "
                "e.occurred_at, e.payload, e.correlation_id, e.causation_id, e.actor_id, e.tenant_id "
                "FROM event_stream e JOIN projection_failure f ON f.event_id=e.event_id "
                "WHERE f.consumer_name=%s AND e.event_id=%s AND f.quarantined_at IS NOT NULL AND f.resolved_at IS NULL",
                ("auction-overview-v1", event_id),
            )
            event = cursor.fetchone()
        if event is None:
            raise SystemExit("no unresolved quarantined event matches that id")
        envelope = {
            key: (value.isoformat() if key == "occurred_at" else str(value) if key == "event_id" else value)
            for key, value in event.items()
        }
        producer = AIOKafkaProducer(bootstrap_servers=bootstrap, acks="all")
        await producer.start()
        try:
            await producer.send_and_wait(
                "thyphon.domain-events", key=envelope["stream_id"].encode(),
                value=json.dumps(envelope, default=str, sort_keys=True).encode(),
            )
        finally:
            await producer.stop()
        with connection.transaction(), connection.cursor() as cursor:
            cursor.execute(
                "UPDATE projection_failure SET redriven_at=NOW(), redrive_count=redrive_count+1 "
                "WHERE consumer_name=%s AND event_id=%s AND resolved_at IS NULL",
                ("auction-overview-v1", event_id),
            )
    finally:
        connection.close()


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: python -m thyphon.workers.redrive <event-id>")
    asyncio.run(redrive(UUID(sys.argv[1])))


if __name__ == "__main__":
    main()
