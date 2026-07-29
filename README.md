# Thyphon

**An event-sourced commodity allocation exchange and distributed-systems laboratory.**

Thyphon is a terminal-native simulation where autonomous companies compete for finite mineral lots. It is deliberately designed around conditions that make CQRS and event sourcing worthwhile: optimistic conflicts, idempotent commands, at-least-once delivery, projection lag, and a complete reconstructible audit trail.

The TUI uses the terminal's alternate screen buffer (`curses`): it updates in place rather than growing the terminal scrollback. Use `Space` to pause, `+`/`-` to adjust simulation speed, `R` to restart the deterministic seed, and `Q` to quit.

The user-facing product is always **Thyphon**. The lower-case `thyphon` module name is a Python import convention only.

## Delivery 1: advanced operational core

This delivery contains the complete auction path rather than a throwaway MVP:

- Event-sourced `Auction` and `Company` aggregates, rehydrated from every event (no snapshots).
- Intention-led command and fact-led event catalog, with one directory per command/event.
- Optimistic stream versioning, command idempotency, transactional outbox, and idempotent projectors.
- PostgreSQL and Kafka runtime topology in Docker Compose; SQLite test adapter for hermetic Bazel tests.
- Separate read models, projection rebuild command, immutable audit inspection, and deterministic simulation.
- A carefully laid out ASCII operations TUI with market, auction, event tape, and system-health panels.
- Bazel targets for domain, application, TUI, and integration suites.

## Delivery 2: exceptional behaviours and operational depth

- Settlement/contract process manager, late payment compensation, and released-lot reassignment.
- Shipment lifecycle, inventory pressure, market shocks and plug-in agent strategies.
- Kafka consumer restart and dead-letter workflows, fault-injection console, scheduled expiration and reconciliation.
- FastAPI command/query facade, OpenTelemetry, Prometheus/Grafana, load generation and benchmark reporting.

## Run the deterministic terminal simulation

```bash
bazel run //apps/tui -- --ticks 12 --seed 18374
bazel test //...
```

The local simulator has no runtime dependency beyond Python. The production-shaped topology is started separately:

```bash
docker compose up -d postgres kafka
```

## Live API (Delivery 2)

After `docker compose up -d --wait`, Thyphon exposes FastAPI at `http://127.0.0.1:18000`.

```bash
curl http://127.0.0.1:18000/health
curl -X POST http://127.0.0.1:18000/commands/auctions/open \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: open-lithium-381' \
  --data '{"auction_id":"lithium-381","resource":"Lithium","quantity":1200,"reserve_price":"212.00"}'
curl 'http://127.0.0.1:18000/queries/auctions/lithium-381?minimum_version=1'
```

The Compose project is isolated as `thyphon-live`; it creates `thyphon-postgres:phase-2`,
`thyphon-kafka:phase-2`, `thyphon-api:phase-2`, and its own `thyphon-live_thyphon-postgres-data` volume.

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
- Every consumer deduplicates by `(consumer_name, event_id)`.
- The event and outbox record are committed atomically.

See [docs/event-catalog.md](docs/event-catalog.md) and [docs/adr/0001-intention-led-event-sourcing.md](docs/adr/0001-intention-led-event-sourcing.md).
