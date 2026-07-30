#!/bin/zsh
# Verify that a migrated legacy outbox is delivered only after workers start.
set -euo pipefail

cd "${0:A:h}/.."
legacy_stream="legacy-upgrade-auction"

for attempt in {1..30}; do
  projected="$(docker compose exec -T postgres psql -U thyphon -d thyphon -Atc "SELECT count(*) FROM auction_overview WHERE auction_id='${legacy_stream}' AND stream_version=1")"
  raw_failures="$(docker compose exec -T postgres psql -U thyphon -d thyphon -Atc "SELECT count(*) FROM projection_raw_failure WHERE topic='thyphon.domain-events'")"
  if [[ "$projected:$raw_failures" == "1:0" ]]; then
    print "Legacy stream delivery passed after migration: projection is live and no raw DLQ record was created."
    exit 0
  fi
  sleep 1
done

print -u2 "Legacy stream delivery failed: migrated outbox was not projected cleanly."
exit 1
