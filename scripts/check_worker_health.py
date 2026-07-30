"""Compose healthcheck for a worker heartbeat; intentionally no broker probe."""
from __future__ import annotations

import os
import sys


def main() -> None:
    import psycopg

    worker_name = os.environ["THYPHON_WORKER_NAME"]
    with psycopg.connect(os.environ["THYPHON_DATABASE_URL"]) as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT last_beat_at > NOW() - INTERVAL '20 seconds' FROM worker_heartbeat WHERE worker_name=%s",
            (worker_name,),
        )
        row = cursor.fetchone()
    if row is None or not row[0]:
        raise SystemExit(f"worker heartbeat is stale: {worker_name}")


if __name__ == "__main__":
    main()
