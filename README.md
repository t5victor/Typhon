# Thyphon

**An event-sourced commodity allocation exchange and distributed-systems laboratory.**

Thyphon is a terminal-native simulation where autonomous companies compete for finite mineral lots. It is deliberately designed around conditions that make CQRS and event sourcing worthwhile: optimistic conflicts, idempotent commands, at-least-once delivery, projection lag, and a complete reconstructible audit trail.

The TUI uses the terminal's alternate screen buffer (`curses`): it updates in place rather than growing the terminal scrollback. It runs until you press `Q`; use `Space` to pause, `+`/`-` to adjust simulation speed, `R` to restart the current deterministic seed, or `N` to enter a new seed without leaving the console.

The user-facing product is always **Thyphon**. The lower-case `thyphon` module name is a Python import convention only.

## Implemented capabilities

This delivery contains the complete auction path rather than a throwaway MVP:

- Event-sourced `Auction` and `Company` aggregates, rehydrated from every event (no snapshots).
- Intention-led command and fact-led event catalog, with one directory per command/event.
- Optimistic stream versioning, command idempotency, transactional outbox, and idempotent projectors.
- PostgreSQL and Kafka runtime topology in Docker Compose; SQLite test adapter for hermetic Bazel tests.
- Versioned event envelopes with correlation/causation/actor metadata, explicit upcaster seam, and globally ordered replay; Kafka deliveries are checked against the immutable PostgreSQL fact before processing.
- Separate auction read model, coordinated rebuild facility, deterministic simulation, and a poison-event DLQ.
- A curses ASCII TUI with synthetic market telemetry; it does not claim to display Kafka runtime telemetry.
- Bazel targets for domain, application and TUI suites.

## Roadmap — not implemented

- Shipment lifecycle, inventory pressure, market shocks and plug-in agent strategies.
- Prometheus/Grafana dashboards, load generation and benchmark reporting.
- Funds reservation, released-lot reassignment and scheduled auction expiry.

## Run the deterministic terminal simulation

```bash
bazel run //apps:tui -- --ticks 12 --seed 18374
bazel test //...
```

The local simulator has no runtime dependency beyond Python. The production-shaped topology is started separately:

```bash
docker compose up -d --wait
```

## Live API (Delivery 2)

Copy `.env.example` to `.env` and replace both local-only values before `docker compose up -d --wait`.
Thyphon then exposes FastAPI at `http://127.0.0.1:18000`. The API rejects unauthenticated and unauthorized commands;
the supplied `.env.example` identities are strictly for the local lab and are not a production identity solution.

```bash
curl http://127.0.0.1:18000/health
curl -X POST http://127.0.0.1:18000/commands/auctions/open \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: open-lithium-381' \
  -H 'X-Thyphon-API-Key: local-supplier' \
  --data '{"auction_id":"lithium-381","resource":"Lithium","quantity":1200,"reserve_price":"212.00"}'
curl 'http://127.0.0.1:18000/queries/auctions/lithium-381?minimum_version=1'
```

`minimum_version` never blocks an API worker: it returns `202` plus `Retry-After` until its projection catches up.
Run `./scripts/verify_live.zsh` after Compose is ready to exercise authenticated command flow, canonical Kafka delivery,
projection catch-up and event-envelope metadata. The script intentionally uses a fresh, unique auction id each run.

## Repeatable performance baseline

```bash
PYTHONPATH=src python3 scripts/benchmark_simulation.py --ticks 1000 --seed 18374
```

This reports a deterministic local command/event baseline; it is not presented as a distributed throughput claim.

The Compose project is isolated as `thyphon-live`; it creates `thyphon-postgres:phase-2`,
`thyphon-kafka:phase-2`, `thyphon-api:phase-2`, and its own `thyphon-live_thyphon-postgres-data` volume.
The ordered migrations preserve legacy raw stream IDs by classifying and converting them to namespaced streams;
they abort rather than silently merge histories if a target stream would collide.

## One-command launcher

```bash
./scripts/launch_thyphon.zsh
```

It verifies Docker Desktop, starts any missing Thyphon service, shows their status, and opens the
ASCII TUI in a new macOS Terminal window. It preserves the isolated PostgreSQL volume between starts.

## Architectural guardrails

- Commands are verbs with business intent: `OpenAuction`, not `CreateAuction`.
- Events are irreversible business facts: `CompetitiveBidPlaced`, not `BidUpdated`.
- Read models never decide command legality.
- Event streams are append-only; no aggregate snapshots are written or read.
- Every consumer deduplicates by `(consumer_name, event_id)` and quarantines poison messages after bounded retries.
- The event and outbox record are committed atomically.
- Refunds are a two-step workflow: a late settlement requests one refund; only its completion/failure fact resolves it.
- Event streams and outbox envelopes carry schema version, global position, correlation, causation, actor and tenant fields; idempotency receipts are bound to the actor and tenant that created them.

See [docs/event-catalog.md](docs/event-catalog.md) and [docs/adr/0001-intention-led-event-sourcing.md](docs/adr/0001-intention-led-event-sourcing.md).
