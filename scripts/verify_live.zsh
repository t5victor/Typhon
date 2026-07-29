#!/bin/zsh
set -euo pipefail

cd "${0:A:h}/.."
if [[ ! -f .env ]]; then
  print -u2 "Missing .env. Copy .env.example and replace its local values first."
  exit 1
fi

docker compose up -d --wait
base="http://127.0.0.1:18000"
if (( $+commands[uuidgen] )); then
  run_id="$(uuidgen | tr '[:upper:]' '[:lower:]')"
else
  run_id="$(cat /proc/sys/kernel/random/uuid)"
fi
auction_id="live-audit-${run_id}"
open_key="audit-open-${auction_id}"

[[ "$(curl -s -o /dev/null -w '%{http_code}' "${base}/health")" == "200" ]]
[[ "$(curl -s -o /dev/null -w '%{http_code}' "${base}/health/ready")" == "200" ]]
[[ "$(curl -s -o /dev/null -w '%{http_code}' -X POST "${base}/commands/auctions/open")" == "401" ]]

opened="$(curl -fsS -X POST "${base}/commands/auctions/open" \
  -H 'Content-Type: application/json' -H "Idempotency-Key: ${open_key}" \
  -H 'X-Thyphon-API-Key: local-supplier' \
  -H 'X-Correlation-ID: live-audit-correlation' \
  --data "{\"auction_id\":\"${auction_id}\",\"resource\":\"Gold\",\"quantity\":2,\"reserve_price\":\"100.00\"}")"
print -- "$opened" | grep -q '"expected_version":1'

curl -fsS -X POST "${base}/commands/auctions/${auction_id}/competitive-bids" \
  -H 'Content-Type: application/json' -H "Idempotency-Key: audit-bid-${auction_id}" \
  -H 'X-Thyphon-API-Key: local-bidder-nova' \
  --data '{"company_id":"nova-corp","offer":"105.00"}' | grep -q '"expected_version":2'
curl -fsS -X POST "${base}/commands/auctions/${auction_id}/accept-winning-bid" \
  -H "Idempotency-Key: audit-accept-${auction_id}" -H 'X-Thyphon-API-Key: local-operator' | grep -q '"expected_version":3'

for attempt in {1..30}; do
  query="$(curl -s "${base}/queries/auctions/${auction_id}?minimum_version=3")"
  if print -- "$query" | grep -q '"lifecycle":"allocated"'; then
    break
  fi
  sleep 1
done
print -- "$query" | grep -q '"lifecycle":"allocated"'

settlement_id="settlement-${auction_id}"
for attempt in {1..30}; do
  settlement_count="$(docker compose exec -T postgres psql -U thyphon -d thyphon -Atc "SELECT count(*) FROM event_stream WHERE stream_id='settlement:${settlement_id}'")"
  [[ "$settlement_count" == "1" ]] && break
  sleep 1
done
[[ "$settlement_count" == "1" ]]

webhook_secret="$(sed -n 's/^THYPHON_PROVIDER_WEBHOOK_SECRET=//p' .env)"
webhook_timestamp="$(date +%s)"
signature() {
  local timestamp="${4:-$webhook_timestamp}"
  THYPHON_PROVIDER_WEBHOOK_SECRET="$webhook_secret" python3 -c '
import hashlib, hmac, json, os, sys
print(hmac.new(os.environ["THYPHON_PROVIDER_WEBHOOK_SECRET"].encode(), json.dumps({"settlement_id": sys.argv[1], "intention": sys.argv[2], "idempotency_key": sys.argv[3], "timestamp": int(sys.argv[4]), "payload": json.loads(sys.argv[5])}, sort_keys=True, separators=(",", ":")).encode(), hashlib.sha256).hexdigest())
' "$settlement_id" "$1" "$2" "$timestamp" "$3"
}
reject_payload='{"rejection_reason":"local funds release"}'
reject_key="audit-reject-${auction_id}"
bad_callback_payload='{"provider_reference":"invalid-provider-callback"}'
[[ "$(curl -s -o /dev/null -w '%{http_code}' -X POST "${base}/commands/settlements/${settlement_id}/confirm" \
  -H 'Content-Type: application/json' -H "Idempotency-Key: invalid-signature-${auction_id}" -H 'X-Thyphon-API-Key: local-payment-provider' \
  -H "X-Thyphon-Timestamp: ${webhook_timestamp}" -H "X-Thyphon-Signature: $(printf '0%.0s' {1..64})" --data "$bad_callback_payload")" == "401" ]]
expired_timestamp="$((webhook_timestamp - 301))"
[[ "$(curl -s -o /dev/null -w '%{http_code}' -X POST "${base}/commands/settlements/${settlement_id}/confirm" \
  -H 'Content-Type: application/json' -H "Idempotency-Key: expired-signature-${auction_id}" -H 'X-Thyphon-API-Key: local-payment-provider' \
  -H "X-Thyphon-Timestamp: ${expired_timestamp}" -H "X-Thyphon-Signature: $(signature confirm-settlement "expired-signature-${auction_id}" "$bad_callback_payload" "$expired_timestamp")" --data "$bad_callback_payload")" == "401" ]]
curl -fsS -X POST "${base}/commands/settlements/${settlement_id}/reject" \
  -H 'Content-Type: application/json' -H "Idempotency-Key: ${reject_key}" -H 'X-Thyphon-API-Key: local-payment-provider' \
  -H "X-Thyphon-Timestamp: ${webhook_timestamp}" -H "X-Thyphon-Signature: $(signature reject-settlement "$reject_key" "$reject_payload")" --data "$reject_payload" | grep -q '"expected_version":2'
confirm_payload="{\"provider_reference\":\"late-local-provider-${auction_id}\"}"
confirm_key="audit-late-${auction_id}"
curl -fsS -X POST "${base}/commands/settlements/${settlement_id}/confirm" \
  -H 'Content-Type: application/json' -H "Idempotency-Key: ${confirm_key}" -H 'X-Thyphon-API-Key: local-payment-provider' \
  -H "X-Thyphon-Timestamp: ${webhook_timestamp}" -H "X-Thyphon-Signature: $(signature confirm-settlement "$confirm_key" "$confirm_payload")" --data "$confirm_payload" | grep -q '"expected_version":4'
refund_key="audit-refund-${auction_id}"
curl -fsS -X POST "${base}/commands/settlements/${settlement_id}/refund-completed" \
  -H 'Content-Type: application/json' -H "Idempotency-Key: ${refund_key}" -H 'X-Thyphon-API-Key: local-payment-provider' \
  -H "X-Thyphon-Timestamp: ${webhook_timestamp}" -H "X-Thyphon-Signature: $(signature refund-completed "$refund_key" "$confirm_payload")" --data "$confirm_payload" | grep -q '"expected_version":5'
repeat_key="audit-refund-repeat-${auction_id}"
[[ "$(curl -s -o /dev/null -w '%{http_code}' -X POST "${base}/commands/settlements/${settlement_id}/refund-completed" \
  -H 'Content-Type: application/json' -H "Idempotency-Key: ${repeat_key}" -H 'X-Thyphon-API-Key: local-payment-provider' \
  -H "X-Thyphon-Timestamp: ${webhook_timestamp}" -H "X-Thyphon-Signature: $(signature refund-completed "$repeat_key" "$confirm_payload")" --data "$confirm_payload")" == "422" ]]

docker compose exec -T api python -m thyphon.projections.rebuild | grep -q '^Rebuilt auction-overview from [1-9]'

metadata="$(docker compose exec -T postgres psql -U thyphon -d thyphon -Atc \
  "SELECT schema_version || ':' || correlation_id || ':' || global_position FROM event_stream WHERE stream_id='auction:${auction_id}'")"
print -- "$metadata" | grep -q '^1:live-audit-correlation:[1-9]'

# A quarantined record is only resolved after a successful normal consumer
# pass. Re-enqueue an already projected AuctionOpened fact to exercise that
# idempotent redrive workflow without changing market state.
redrive_event="$(docker compose exec -T postgres psql -U thyphon -d thyphon -Atc "SELECT event_id FROM event_stream WHERE stream_id='auction:${auction_id}' ORDER BY stream_version LIMIT 1")"
docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U thyphon -d thyphon -c \
  "INSERT INTO projection_failure(consumer_name, event_id, attempts, last_error, quarantined_at) VALUES ('auction-overview-v1', '${redrive_event}', 3, 'integration redrive check', NOW()) ON CONFLICT (consumer_name, event_id) DO UPDATE SET attempts=3, last_error='integration redrive check', quarantined_at=NOW(), redriven_at=NULL, resolved_at=NULL"
docker compose exec -T api python -m thyphon.workers.redrive "$redrive_event"
for attempt in {1..30}; do
  resolved="$(docker compose exec -T postgres psql -U thyphon -d thyphon -Atc "SELECT resolved_at IS NOT NULL FROM projection_failure WHERE consumer_name='auction-overview-v1' AND event_id='${redrive_event}'")"
  [[ "$resolved" == "t" ]] && break
  sleep 1
done
[[ "$resolved" == "t" ]]

print "Live verification passed: auth, PostgreSQL, Kafka readiness, outbox/projection/rebuild, envelopes, late settlement, one-time refund and DLQ redrive."
