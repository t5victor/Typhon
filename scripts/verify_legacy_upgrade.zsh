#!/bin/zsh
# Exercise migration 005 against records shaped like the first Thyphon commit.
set -euo pipefail

cd "${0:A:h}/.."
legacy_stream="legacy-upgrade-auction"
legacy_event="00000000-0000-4000-8000-000000000005"
legacy_settlement_stream="legacy-upgrade-settlement"
legacy_settlement_event="00000000-0000-4000-8000-000000000006"

docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U thyphon -d thyphon <<SQL
TRUNCATE transactional_outbox, command_receipt, event_stream_head, event_stream,
  provider_reference_claim, settlement_causation_claim, projection_receipt, projection_failure;
DELETE FROM schema_migration WHERE migration_id IN ('005_namespace_legacy_streams_and_outbox', '006_bind_idempotency_receipts_to_actor_and_tenant', '007_claim_each_winning_bid_once', '008_track_dlq_redrives');
INSERT INTO event_stream(event_id, stream_id, stream_version, event_name, payload, occurred_at, schema_version, correlation_id)
VALUES ('$legacy_event', '$legacy_stream', 1, 'AuctionOpened',
  '{"event_id":"$legacy_event","occurred_at":"2026-01-01T00:00:00+00:00","resource":"Copper","quantity":4,"reserve_price":"101.00"}'::jsonb,
  '2026-01-01T00:00:00+00:00', 1, 'legacy-upgrade-correlation');
INSERT INTO event_stream_head(stream_id, current_version) VALUES ('$legacy_stream', 1);
INSERT INTO command_receipt(idempotency_key, stream_id, command_name, request_hash, actor_id, tenant_id, resulting_version, accepted_at)
VALUES ('legacy-upgrade-key', '$legacy_stream', 'OpenAuction', 'legacy-hash', '', '', 1, NOW());
INSERT INTO transactional_outbox(event_id, topic, partition_key, body)
VALUES ('$legacy_event', 'thyphon.domain-events', '$legacy_stream', '{"event_id":"$legacy_event","stream_id":"$legacy_stream"}'::jsonb);
INSERT INTO event_stream(event_id, stream_id, stream_version, event_name, payload, occurred_at, schema_version, correlation_id)
VALUES ('$legacy_settlement_event', '$legacy_settlement_stream', 1, 'SettlementRequested',
  '{"event_id":"$legacy_settlement_event","occurred_at":"2026-01-01T00:00:00+00:00","auction_id":"legacy-upgrade-auction","payer_company_id":"nova","amount":"101.00"}'::jsonb,
  '2026-01-01T00:00:00+00:00', 1, 'legacy-upgrade-correlation');
INSERT INTO event_stream_head(stream_id, current_version) VALUES ('$legacy_settlement_stream', 1);
INSERT INTO command_receipt(idempotency_key, stream_id, command_name, request_hash, actor_id, tenant_id, resulting_version, accepted_at)
VALUES ('legacy-settlement-key', '$legacy_settlement_stream', 'RequestSettlement', 'legacy-hash', '', '', 1, NOW());
INSERT INTO provider_reference_claim(provider_reference, settlement_stream_id)
VALUES ('legacy-upgrade-provider-reference', '$legacy_settlement_stream');
INSERT INTO transactional_outbox(event_id, topic, partition_key, body)
VALUES ('$legacy_settlement_event', 'thyphon.domain-events', '$legacy_settlement_stream', '{"event_id":"$legacy_settlement_event","stream_id":"$legacy_settlement_stream"}'::jsonb);
SQL

docker compose exec -T api python -m thyphon.infrastructure.migrate

stream_count="$(docker compose exec -T postgres psql -U thyphon -d thyphon -Atc "SELECT count(*) FROM event_stream WHERE stream_id='auction:${legacy_stream}'")"
head_count="$(docker compose exec -T postgres psql -U thyphon -d thyphon -Atc "SELECT count(*) FROM event_stream_head WHERE stream_id='auction:${legacy_stream}'")"
receipt_count="$(docker compose exec -T postgres psql -U thyphon -d thyphon -Atc "SELECT count(*) FROM command_receipt WHERE stream_id='auction:${legacy_stream}'")"
outbox_count="$(docker compose exec -T postgres psql -U thyphon -d thyphon -Atc "SELECT count(*) FROM transactional_outbox WHERE (partition_key='auction:${legacy_stream}' AND body->>'stream_id'='auction:${legacy_stream}') OR (partition_key='settlement:${legacy_settlement_stream}' AND body->>'stream_id'='settlement:${legacy_settlement_stream}')")"
provider_claim_count="$(docker compose exec -T postgres psql -U thyphon -d thyphon -Atc "SELECT count(*) FROM provider_reference_claim WHERE provider_reference='legacy-upgrade-provider-reference' AND settlement_stream_id='settlement:${legacy_settlement_stream}'")"
[[ "$stream_count:$head_count:$receipt_count:$outbox_count:$provider_claim_count" == "1:1:1:2:1" ]]

print "Legacy stream upgrade passed: event streams, heads, receipts, provider claim and outboxes were namespaced together."
