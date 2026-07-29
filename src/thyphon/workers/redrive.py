"""Queue a durable, auditable redrive attempt for one quarantined event."""
from __future__ import annotations

import asyncio
import json
import os
import sys
from uuid import UUID, uuid4


async def redrive(event_id: UUID) -> None:
    import psycopg

    dsn = os.environ["THYPHON_DATABASE_URL"]
    connection = psycopg.connect(dsn, row_factory=psycopg.rows.dict_row, autocommit=True)
    try:
        # Persist the attempt before any dispatcher can publish it. The worker
        # carries attempt_id in a Kafka header, so completion cannot race this
        # bookkeeping write.
        with connection.transaction(), connection.cursor() as cursor:
            cursor.execute(
                "SELECT e.event_id, e.event_name, e.schema_version, e.stream_id, e.stream_version, e.global_position, "
                "e.occurred_at, e.payload, e.correlation_id, e.causation_id, e.actor_id, e.tenant_id "
                "FROM event_stream e JOIN projection_failure f ON f.event_id=e.event_id "
                "WHERE f.consumer_name=%s AND e.event_id=%s AND f.quarantined_at IS NOT NULL AND f.resolved_at IS NULL "
                "FOR UPDATE OF f",
                ("auction-overview-v1", event_id),
            )
            event = cursor.fetchone()
            if event is None:
                raise SystemExit("no unresolved quarantined event matches that id")
            envelope = {
                key: (value.isoformat() if key == "occurred_at" else str(value) if key == "event_id" else value)
                for key, value in event.items()
            }
            attempt_id = uuid4()
            cursor.execute(
                "INSERT INTO projection_redrive_attempt(attempt_id, consumer_name, event_id, envelope) "
                "VALUES (%s, %s, %s, %s::jsonb)",
                (attempt_id, "auction-overview-v1", event_id, json.dumps(envelope, default=str, sort_keys=True)),
            )
            cursor.execute(
                "UPDATE projection_failure SET redriven_at=NOW(), redrive_count=redrive_count+1, "
                "active_redrive_attempt_id=%s WHERE consumer_name=%s AND event_id=%s AND resolved_at IS NULL",
                (attempt_id, "auction-overview-v1", event_id),
            )
        print(f"Queued redrive attempt {attempt_id} for {event_id}")
    finally:
        connection.close()


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: python -m thyphon.workers.redrive <event-id>")
    asyncio.run(redrive(UUID(sys.argv[1])))


if __name__ == "__main__":
    main()
