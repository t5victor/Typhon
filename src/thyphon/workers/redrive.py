"""Queue a durable, auditable redrive attempt for one quarantined event."""
from __future__ import annotations

import json
import os
import sys
from uuid import UUID, uuid4


DEFAULT_CONSUMER = "auction-overview-v1"


def redrive(event_id: UUID, *, requested_by: str, reason: str, consumer_name: str = DEFAULT_CONSUMER) -> None:
    import psycopg

    dsn = os.environ["THYPHON_DATABASE_URL"]
    connection = psycopg.connect(dsn, row_factory=psycopg.rows.dict_row, autocommit=True)
    try:
        # The failure row serializes operators. Reusing an already active
        # attempt makes the administrative command idempotent and prevents an
        # older Kafka delivery from being orphaned by a newer request.
        with connection.transaction(), connection.cursor() as cursor:
            cursor.execute(
                "SELECT e.event_id, e.event_name, e.schema_version, e.stream_id, e.stream_version, e.global_position, "
                "e.occurred_at, e.payload, e.correlation_id, e.causation_id, e.actor_id, e.tenant_id "
                "FROM event_stream e JOIN projection_failure f ON f.event_id=e.event_id "
                "WHERE f.consumer_name=%s AND e.event_id=%s AND f.quarantined_at IS NOT NULL AND f.resolved_at IS NULL "
                "FOR UPDATE OF f",
                (consumer_name, event_id),
            )
            event = cursor.fetchone()
            if event is None:
                raise SystemExit("no unresolved quarantined event matches that id")
            cursor.execute(
                "SELECT a.attempt_id FROM projection_redrive_attempt a "
                "WHERE a.consumer_name=%s AND a.event_id=%s AND a.status IN ('pending', 'published') "
                "ORDER BY a.requested_at DESC LIMIT 1 FOR UPDATE",
                (consumer_name, event_id),
            )
            active_attempt = cursor.fetchone()
            if active_attempt is not None:
                print(f"Redrive attempt {active_attempt['attempt_id']} is already active for {event_id}")
                return
            envelope = {
                key: (value.isoformat() if key == "occurred_at" else str(value) if key == "event_id" else value)
                for key, value in event.items()
            }
            attempt_id = uuid4()
            cursor.execute(
                "INSERT INTO projection_redrive_attempt("
                "attempt_id, consumer_name, event_id, envelope, status, requested_by, reason) "
                "VALUES (%s, %s, %s, %s::jsonb, 'pending', %s, %s)",
                (
                    attempt_id, consumer_name, event_id, json.dumps(envelope, default=str, sort_keys=True),
                    requested_by, reason,
                ),
            )
            cursor.execute(
                "UPDATE projection_failure SET redriven_at=NOW(), redrive_count=redrive_count+1, "
                "active_redrive_attempt_id=%s WHERE consumer_name=%s AND event_id=%s AND resolved_at IS NULL",
                (attempt_id, consumer_name, event_id),
            )
        print(f"Queued redrive attempt {attempt_id} for {event_id}")
    finally:
        connection.close()


def main() -> None:
    if not 2 <= len(sys.argv) <= 4:
        raise SystemExit("usage: python -m thyphon.workers.redrive <event-id> [operator] [reason]")
    requested_by = sys.argv[2] if len(sys.argv) >= 3 else os.environ.get("THYPHON_REDRIVE_OPERATOR", "operator")
    reason = sys.argv[3] if len(sys.argv) == 4 else os.environ.get("THYPHON_REDRIVE_REASON", "manual redrive")
    redrive(
        UUID(sys.argv[1]), requested_by=requested_by, reason=reason,
        consumer_name=os.environ.get("THYPHON_REDRIVE_CONSUMER", DEFAULT_CONSUMER),
    )


if __name__ == "__main__":
    main()
