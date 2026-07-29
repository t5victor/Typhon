"""Small, ordered SQL migration runner for the local Compose topology."""
from __future__ import annotations

import os


MIGRATIONS = {
    "001_harden_command_receipts": """
      ALTER TABLE command_receipt ADD COLUMN IF NOT EXISTS command_name TEXT;
      ALTER TABLE command_receipt ADD COLUMN IF NOT EXISTS request_hash TEXT;
      UPDATE command_receipt SET command_name = COALESCE(command_name, 'legacy-command'), request_hash = COALESCE(request_hash, 'legacy-receipt');
      ALTER TABLE command_receipt ALTER COLUMN command_name SET NOT NULL;
      ALTER TABLE command_receipt ALTER COLUMN request_hash SET NOT NULL;
      CREATE TABLE IF NOT EXISTS provider_reference_claim (
        provider_reference TEXT PRIMARY KEY, settlement_stream_id TEXT NOT NULL
      );
    """,
    "002_stream_heads_and_projection_failures": """
      CREATE TABLE IF NOT EXISTS event_stream_head (
        stream_id TEXT PRIMARY KEY, current_version BIGINT NOT NULL DEFAULT 0
      );
      INSERT INTO event_stream_head(stream_id, current_version)
      SELECT stream_id, MAX(stream_version) FROM event_stream GROUP BY stream_id
      ON CONFLICT (stream_id) DO UPDATE SET current_version = EXCLUDED.current_version;
      CREATE TABLE IF NOT EXISTS projection_failure (
        consumer_name TEXT NOT NULL, event_id UUID NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
        last_error TEXT NOT NULL, quarantined_at TIMESTAMPTZ,
        PRIMARY KEY (consumer_name, event_id)
      );
    """,
    "003_event_contract_and_trace_metadata": """
      ALTER TABLE event_stream ADD COLUMN IF NOT EXISTS schema_version SMALLINT NOT NULL DEFAULT 1;
      ALTER TABLE event_stream ADD COLUMN IF NOT EXISTS correlation_id TEXT;
      ALTER TABLE event_stream ADD COLUMN IF NOT EXISTS causation_id TEXT;
      ALTER TABLE event_stream ADD COLUMN IF NOT EXISTS actor_id TEXT;
      ALTER TABLE event_stream ADD COLUMN IF NOT EXISTS tenant_id TEXT;
      UPDATE event_stream SET correlation_id = event_id::text WHERE correlation_id IS NULL;
      ALTER TABLE event_stream ALTER COLUMN correlation_id SET NOT NULL;
    """,
    "004_global_event_position_for_legacy_streams": """
      DO $$
      BEGIN
        IF NOT EXISTS (
          SELECT 1 FROM information_schema.columns
          WHERE table_name='event_stream' AND column_name='global_position'
        ) THEN
          CREATE SEQUENCE event_stream_global_position_seq;
          ALTER TABLE event_stream ADD COLUMN global_position BIGINT;
          UPDATE event_stream SET global_position=nextval('event_stream_global_position_seq') WHERE global_position IS NULL;
          ALTER TABLE event_stream ALTER COLUMN global_position SET NOT NULL;
          ALTER TABLE event_stream ALTER COLUMN global_position SET DEFAULT nextval('event_stream_global_position_seq');
          CREATE UNIQUE INDEX event_stream_global_position_key ON event_stream(global_position);
        END IF;
      END $$;
    """,
}


def main() -> None:
    import psycopg

    connection = psycopg.connect(os.environ["THYPHON_DATABASE_URL"], autocommit=True)
    with connection.cursor() as cursor:
        cursor.execute("CREATE TABLE IF NOT EXISTS schema_migration (migration_id TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW())")
        for migration_id, sql in MIGRATIONS.items():
            cursor.execute("SELECT 1 FROM schema_migration WHERE migration_id=%s", (migration_id,))
            if cursor.fetchone() is None:
                getattr(cursor, "execute")(sql)
                cursor.execute("INSERT INTO schema_migration(migration_id) VALUES (%s)", (migration_id,))
    connection.close()


if __name__ == "__main__":
    main()
