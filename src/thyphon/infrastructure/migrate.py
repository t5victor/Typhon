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
    "005_namespace_legacy_streams_and_outbox": """
      CREATE TABLE IF NOT EXISTS legacy_stream_namespace_map (
        legacy_stream_id TEXT PRIMARY KEY,
        namespaced_stream_id TEXT NOT NULL UNIQUE,
        aggregate_type TEXT NOT NULL,
        migrated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
      WITH classified AS (
        SELECT stream_id,
          CASE
            WHEN bool_and(event_name = ANY (ARRAY['AuctionOpened','CompetitiveBidPlaced','WinningBidAccepted','AuctionExpired']::TEXT[])) THEN 'auction'
            WHEN bool_and(event_name = ANY (ARRAY['CompanyOnboarded','RiskAppetiteChanged']::TEXT[])) THEN 'company'
            WHEN bool_and(event_name = ANY (ARRAY['SettlementRequested','SettlementConfirmed','SettlementRejected','LateSettlementDetected','RefundRequested','RefundCompleted','RefundFailed']::TEXT[])) THEN 'settlement'
            ELSE NULL
          END AS aggregate_type
        FROM event_stream
        WHERE stream_id NOT LIKE '%:%'
        GROUP BY stream_id
      )
      INSERT INTO legacy_stream_namespace_map(legacy_stream_id, namespaced_stream_id, aggregate_type)
      SELECT stream_id, aggregate_type || ':' || stream_id, aggregate_type
      FROM classified
      WHERE aggregate_type IS NOT NULL
      ON CONFLICT (legacy_stream_id) DO NOTHING;
      DO $$
      BEGIN
        IF EXISTS (
          SELECT 1 FROM event_stream e
          JOIN legacy_stream_namespace_map m ON m.namespaced_stream_id=e.stream_id
          WHERE e.stream_id <> m.legacy_stream_id
        ) THEN
          RAISE EXCEPTION 'cannot namespace legacy streams: a target stream already exists';
        END IF;
        IF EXISTS (
          SELECT 1 FROM event_stream_head h
          JOIN legacy_stream_namespace_map m ON m.namespaced_stream_id=h.stream_id
          WHERE h.stream_id <> m.legacy_stream_id
        ) THEN
          RAISE EXCEPTION 'cannot namespace legacy streams: a target stream head already exists';
        END IF;
        IF EXISTS (
          SELECT 1 FROM event_stream_head h
          WHERE h.stream_id NOT LIKE '%:%'
            AND NOT EXISTS (SELECT 1 FROM legacy_stream_namespace_map m WHERE m.legacy_stream_id=h.stream_id)
        ) THEN
          RAISE EXCEPTION 'cannot namespace a legacy stream head without classified events';
        END IF;
      END $$;
      UPDATE event_stream e SET stream_id=m.namespaced_stream_id
      FROM legacy_stream_namespace_map m WHERE e.stream_id=m.legacy_stream_id;
      UPDATE event_stream_head h SET stream_id=m.namespaced_stream_id
      FROM legacy_stream_namespace_map m WHERE h.stream_id=m.legacy_stream_id;
      UPDATE command_receipt r SET stream_id=m.namespaced_stream_id
      FROM legacy_stream_namespace_map m WHERE r.stream_id=m.legacy_stream_id;
      UPDATE provider_reference_claim p SET settlement_stream_id=m.namespaced_stream_id
      FROM legacy_stream_namespace_map m WHERE p.settlement_stream_id=m.legacy_stream_id;
      UPDATE transactional_outbox o
      SET partition_key=e.stream_id,
          body=jsonb_build_object(
            'event_id', e.event_id::TEXT, 'event_name', e.event_name,
            'schema_version', e.schema_version, 'stream_id', e.stream_id,
            'stream_version', e.stream_version, 'global_position', e.global_position,
            'occurred_at', e.occurred_at, 'payload', e.payload,
            'correlation_id', e.correlation_id, 'causation_id', e.causation_id,
            'actor_id', e.actor_id, 'tenant_id', e.tenant_id
          )
      FROM event_stream e WHERE e.event_id=o.event_id;
    """,
    "006_bind_idempotency_receipts_to_actor_and_tenant": """
      ALTER TABLE command_receipt ADD COLUMN IF NOT EXISTS actor_id TEXT;
      ALTER TABLE command_receipt ADD COLUMN IF NOT EXISTS tenant_id TEXT;
      UPDATE command_receipt SET actor_id=COALESCE(actor_id, ''), tenant_id=COALESCE(tenant_id, '');
      ALTER TABLE command_receipt ALTER COLUMN actor_id SET NOT NULL;
      ALTER TABLE command_receipt ALTER COLUMN tenant_id SET NOT NULL;
    """,
    "007_claim_each_winning_bid_once": """
      CREATE TABLE IF NOT EXISTS settlement_causation_claim (
        winning_bid_event_id UUID PRIMARY KEY, settlement_stream_id TEXT NOT NULL
      );
      DO $$
      BEGIN
        IF EXISTS (
          SELECT payload->>'winning_bid_event_id'
          FROM event_stream
          WHERE event_name='SettlementRequested'
            AND payload ? 'winning_bid_event_id'
            AND payload->>'winning_bid_event_id' IS NOT NULL
          GROUP BY payload->>'winning_bid_event_id'
          HAVING COUNT(*) > 1
        ) THEN
          RAISE EXCEPTION 'cannot claim winning bids: historic SettlementRequested events are duplicated';
        END IF;
      END $$;
      INSERT INTO settlement_causation_claim(winning_bid_event_id, settlement_stream_id)
      SELECT (payload->>'winning_bid_event_id')::UUID, stream_id
      FROM event_stream
      WHERE event_name='SettlementRequested'
        AND payload ? 'winning_bid_event_id'
        AND payload->>'winning_bid_event_id' IS NOT NULL
      ON CONFLICT (winning_bid_event_id) DO NOTHING;
    """,
    "008_track_dlq_redrives": """
      ALTER TABLE projection_failure ADD COLUMN IF NOT EXISTS redriven_at TIMESTAMPTZ;
      ALTER TABLE projection_failure ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMPTZ;
      ALTER TABLE projection_failure ADD COLUMN IF NOT EXISTS redrive_count INTEGER NOT NULL DEFAULT 0;
    """,
    "009_validate_causality_and_redrive_outbox": """
      DO $$
      BEGIN
        IF EXISTS (
          SELECT payload->>'winning_bid_event_id'
          FROM event_stream
          WHERE event_name='SettlementRequested'
            AND payload ? 'winning_bid_event_id'
            AND payload->>'winning_bid_event_id' IS NOT NULL
          GROUP BY payload->>'winning_bid_event_id'
          HAVING COUNT(*) > 1
        ) THEN
          RAISE EXCEPTION 'cannot claim winning bids: historic SettlementRequested events are duplicated';
        END IF;
      END $$;
      ALTER TABLE projection_failure ADD COLUMN IF NOT EXISTS active_redrive_attempt_id UUID;
      CREATE TABLE IF NOT EXISTS projection_raw_failure (
        consumer_name TEXT NOT NULL, topic TEXT NOT NULL, partition_id INTEGER NOT NULL,
        message_offset BIGINT NOT NULL, raw_value BYTEA, attempts INTEGER NOT NULL DEFAULT 0,
        last_error TEXT NOT NULL, quarantined_at TIMESTAMPTZ,
        PRIMARY KEY (consumer_name, topic, partition_id, message_offset)
      );
      CREATE TABLE IF NOT EXISTS projection_redrive_attempt (
        attempt_id UUID PRIMARY KEY, consumer_name TEXT NOT NULL, event_id UUID NOT NULL,
        envelope JSONB NOT NULL, requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        published_at TIMESTAMPTZ, resolved_at TIMESTAMPTZ
      );
    """,
    "010_bind_settlement_causality_to_winning_bid_facts": """
      DO $$
      BEGIN
        IF EXISTS (
          SELECT 1
          FROM settlement_causation_claim c
          LEFT JOIN event_stream w ON w.event_id=c.winning_bid_event_id
          LEFT JOIN event_stream s ON s.stream_id=c.settlement_stream_id AND s.event_name='SettlementRequested'
          WHERE w.event_id IS NULL
             OR w.event_name <> 'WinningBidAccepted'
             OR w.stream_id NOT LIKE 'auction:%'
             OR s.event_id IS NULL
             OR s.payload->>'auction_id' <> substring(w.stream_id FROM 9)
             OR s.payload->>'payer_company_id' <> w.payload->>'company_id'
             OR s.payload->>'amount' <> w.payload->>'accepted_offer'
             OR s.payload->>'winning_bid_event_id' IS DISTINCT FROM c.winning_bid_event_id::TEXT
        ) THEN
          RAISE EXCEPTION 'cannot bind Settlement causality: an existing claim does not match a WinningBidAccepted fact';
        END IF;
        IF NOT EXISTS (
          SELECT 1 FROM pg_constraint
          WHERE conname='settlement_causation_claim_winning_bid_event_id_fkey'
        ) THEN
          ALTER TABLE settlement_causation_claim
          ADD CONSTRAINT settlement_causation_claim_winning_bid_event_id_fkey
          FOREIGN KEY (winning_bid_event_id) REFERENCES event_stream(event_id);
        END IF;
      END $$;
    """,
    "011_verify_historic_settlement_causal_payload_binding": """
      DO $$
      BEGIN
        IF EXISTS (
          SELECT 1
          FROM settlement_causation_claim c
          LEFT JOIN event_stream w ON w.event_id=c.winning_bid_event_id
          LEFT JOIN event_stream s ON s.stream_id=c.settlement_stream_id AND s.event_name='SettlementRequested'
          WHERE w.event_id IS NULL
             OR w.event_name <> 'WinningBidAccepted'
             OR w.stream_id NOT LIKE 'auction:%'
             OR s.event_id IS NULL
             OR s.payload->>'auction_id' <> substring(w.stream_id FROM 9)
             OR s.payload->>'payer_company_id' <> w.payload->>'company_id'
             OR s.payload->>'amount' <> w.payload->>'accepted_offer'
             OR s.payload->>'winning_bid_event_id' IS DISTINCT FROM c.winning_bid_event_id::TEXT
        ) THEN
          RAISE EXCEPTION 'cannot verify Settlement causality: a historic claim is not bound to its event payload';
        END IF;
      END $$;
    """,
}


def main() -> None:
    import psycopg

    connection = psycopg.connect(os.environ["THYPHON_DATABASE_URL"])
    try:
        with connection.transaction(), connection.cursor() as cursor:
            cursor.execute("CREATE TABLE IF NOT EXISTS schema_migration (migration_id TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW())")
        for migration_id, sql in MIGRATIONS.items():
            # A migration and its receipt are one transaction. In particular,
            # the legacy namespace conversion either completes as a whole or
            # leaves the previous event-store shape untouched.
            with connection.transaction(), connection.cursor() as cursor:
                cursor.execute("SELECT 1 FROM schema_migration WHERE migration_id=%s", (migration_id,))
                if cursor.fetchone() is None:
                    cursor.execute(sql)
                    cursor.execute("INSERT INTO schema_migration(migration_id) VALUES (%s)", (migration_id,))
    finally:
        connection.close()


if __name__ == "__main__":
    main()
