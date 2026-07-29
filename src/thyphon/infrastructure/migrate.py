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
}


def main() -> None:
    import psycopg

    connection = psycopg.connect(os.environ["THYPHON_DATABASE_URL"], autocommit=True)
    with connection.cursor() as cursor:
        cursor.execute("CREATE TABLE IF NOT EXISTS schema_migration (migration_id TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW())")
        for migration_id, sql in MIGRATIONS.items():
            cursor.execute("SELECT 1 FROM schema_migration WHERE migration_id=%s", (migration_id,))
            if cursor.fetchone() is None:
                cursor.execute(sql)
                cursor.execute("INSERT INTO schema_migration(migration_id) VALUES (%s)", (migration_id,))
    connection.close()


if __name__ == "__main__":
    main()
