# Thyphon technical architecture

Thyphon is an event-sourced commodity auction simulator. The domain makes
distributed-systems behaviour visible: concurrent bids, immutable decisions,
at-least-once delivery, late callbacks and projections rebuilt from first
principles.

It is a laboratory, not a claim of production scale. The useful part is the
shape of the guarantees, the failure modes and the operational trade-offs.

## System map

~~~mermaid
flowchart LR
    client["Command client"] --> api["FastAPI"]
    api --> handler["Command handler"]
    handler --> aggregate["Rehydrated aggregate"]
    aggregate --> eventStore[("PostgreSQL event stream")]
    eventStore --> outbox[("Transactional outbox")]
    outbox --> dispatcher["Outbox dispatcher"]
    dispatcher --> kafka["Kafka"]
    kafka --> projector["Projection worker"]
    projector --> overview[("auction_overview")]
    kafka --> manager["Settlement process manager"]
    manager --> settlement["Settlement stream"]
    settlement --> outbox
    provider["Payment provider"] --> callback["Signed callback"]
    callback --> api

    classDef edge fill:#172033,stroke:#7dd3fc,color:#e0f2fe,stroke-width:2px;
    classDef domain fill:#312e81,stroke:#c4b5fd,color:#f5f3ff,stroke-width:2px;
    classDef storage fill:#14332a,stroke:#6ee7b7,color:#ecfdf5,stroke-width:2px;
    classDef transport fill:#3b2512,stroke:#fbbf24,color:#fffbeb,stroke-width:2px;
    classDef projection fill:#3b173f,stroke:#f0abfc,color:#fdf4ff,stroke-width:2px;
    class client,api,provider,callback edge;
    class handler,aggregate,manager domain;
    class eventStore,outbox,overview,settlement storage;
    class dispatcher,kafka transport;
    class projector projection;
~~~

The write path accepts a decision only after PostgreSQL commits the event and
its outbox record in one transaction. Kafka distributes committed facts; it is
not the authority for a financial obligation.

## Domain language

Commands express a decision. Events express an irreversible fact. Generic
Create, Update, Delete and SetStatus operations are avoided because they hide
the action that matters when replaying history.

| Aggregate | Decisions | Facts |
|---|---|---|
| Auction | OpenAuction, PlaceCompetitiveBid, AcceptWinningBid, ExpireAuction | AuctionOpened, CompetitiveBidPlaced, WinningBidAccepted, AuctionExpired |
| Company | OnboardCompany, ChangeRiskAppetite | CompanyOnboarded, RiskAppetiteChanged |
| Settlement | RequestSettlement, ConfirmSettlement, RejectSettlement, CompleteRefund, FailRefund | SettlementRequested, SettlementConfirmed, SettlementRejected, LateSettlementDetected, RefundRequested, RefundCompleted, RefundFailed |

Every command and event owns its directory. Tests live beside the aggregate or
projection they exercise rather than inside a command directory.

### Auction

- A lot opens once with a positive quantity and reserve price.
- A competitive bid must beat the current leader.
- Only the leader can be accepted.
- An allocated or expired auction cannot accept another bid.

### Settlement

Settlement is never constructed from client-supplied money. A new
SettlementRequested cites a canonical WinningBidAccepted event from the same
Auction, company and offer. PostgreSQL enforces one Settlement claim per winning
bid and references the causal event through a foreign key.

Late confirmation after rejection enters the compensation path:

~~~text
SettlementRejected
        ↓
LateSettlementDetected
        ↓
RefundRequested(provider_reference)
        ↓
RefundCompleted | RefundFailed
~~~

Completion or failure must carry the provider reference from RefundRequested.

## Event store and concurrency

event_stream is append-only. Each row has an event ID, namespaced stream ID,
stream version, global position, event name, payload, occurrence time and trace
metadata: correlation_id, causation_id, actor_id and tenant_id.

~~~text
auction:{auction_id}
company:{company_id}
settlement:{settlement_id}
~~~

event_stream_head stores the current version for each stream. An append takes
place in one PostgreSQL transaction:

1. acquire an advisory lock derived from the idempotency key;
2. read an existing receipt while holding that lock;
3. lock the stream head and compare versions;
4. append events, outbox envelopes, head and receipt;
5. commit.

The unique stream_id + stream_version constraint is the final physical barrier.
A concurrent loser receives an optimistic-concurrency conflict rather than
silently changing the auction.

### Idempotency

command_receipt binds an idempotency key to stream, command name, canonical
request hash, actor and tenant. A retry from the same identity returns the
recorded version. Reusing the key for another request, stream or identity is a
domain conflict.

This is business idempotency, not exactly-once transport. Kafka can duplicate a
record; projectors and process managers remain safe when it does.

## Transactional outbox and Kafka

transactional_outbox is written with the event. The dispatcher reads pending
records in global order, publishes them to Kafka and then records published_at.
If Kafka accepts a message and PostgreSQL fails before that mark, the next
attempt republishes the immutable event. Consumers are therefore idempotent by
design.

One logical outbox dispatcher publishes at a time. A PostgreSQL advisory lock
prevents horizontally scaled replicas from publishing a later global position
before an earlier one. Dispatcher connections are discarded and recreated after
infrastructure failures such as a PostgreSQL restart.

Kafka records are verified against the canonical PostgreSQL row before they are
projected or allowed to start Settlement. The check covers event ID, stream,
version, global position, payload and trace metadata.

## Projections and consistency

auction_overview is the Auction query model. Command handlers do not read it to
decide legality.

The projector tracks consumer_name + event_id receipts and requires each Auction
stream version to follow its predecessor. A version gap is not receipted as
success. It is quarantined and later repaired by rebuilding that stream from the
Event Store in canonical order.

The full rebuild uses a shadow table under the same advisory lock as the live
projector. Readers see the old projection until the final table swap; they do
not observe an empty or partially replayed model.

Queries can request minimum_version. If the projection has not caught up, the
API returns 202 Accepted with Retry-After instead of blocking an API worker.

## Redrive and dead-letter control plane

Malformed records, invalid contracts and deterministic projection errors are
retried a bounded number of times. Infrastructure failures back off, seek to the
same offset and leave it uncommitted.

### Quarantine

Quarantine writes two things in one transaction:

1. projection_failure for a canonical event or projection_raw_failure for a
   record that cannot be trusted as an event;
2. a projection_dead_letter_outbox intent.

The DLQ dispatcher publishes a bounded reference: source coordinates,
dead-letter ID, SHA-256, byte size, error and limited Base64 preview. Raw input
is not copied into Kafka. A large poison message cannot exceed Kafka request
size merely because it is being quarantined.

### Redrive lifecycle

~~~mermaid
stateDiagram-v2
    [*] --> pending
    pending --> published
    published --> resolved
    pending --> failed
    published --> failed
    pending --> superseded
    published --> superseded
    resolved --> [*]
    failed --> [*]
    superseded --> [*]
~~~

Queue a repair with:

~~~bash
python -m thyphon.workers.redrive <event-id> [operator] [reason]
~~~

projection_redrive_attempt carries operator, reason, lifecycle and last error.
A partial unique index permits one pending or published attempt per failure.
Repeating a request returns the active attempt instead of creating an orphan. A
terminal duplicate delivery is a no-op; an unknown or mismatched attempt is
quarantined.

## Event contracts and migrations

Envelopes include schema_version. Readers reject versions below one or newer
than the supported contract. Upcasting is explicit and local to the event
adapter. Historical SettlementRequested v1 is readable with a missing causal
winning-bid ID; current commands require one.

Migrations are ordered and recorded in schema_migration. The legacy stream
migration converts raw stream IDs to namespaced IDs and updates stream heads,
command receipts, provider claims and outbox envelopes in one transaction. It
aborts on collisions rather than merging histories.

CI runs this upgrade in two stages: PostgreSQL alone seeds and migrates the
legacy shape; workers start only after the outbox body has been converted; the
delivery is then verified.

## Local topology

| Service | Responsibility |
|---|---|
| postgres | event store, receipts, outboxes and read models |
| kafka | event transport |
| migrate | ordered schema migrations |
| api | authenticated command and query boundary |
| outbox-worker | canonical event publication |
| projection-worker | Auction projection and Settlement process manager |
| redrive-outbox-worker | redrive attempt publication |
| dead-letter-outbox-worker | bounded DLQ reference publication |

The TUI stays independent of this topology. It uses a deterministic SQLite
simulator so a seed can reproduce a market session without containers.

## Security model

The local API uses API-key identities with actor, role and tenant metadata.
Roles gate Auction and Settlement commands. Tenant is trace metadata today, not
tenant isolation; cross-tenant ownership boundaries remain future work.

Payment callbacks are HMAC-signed over settlement ID, command intention,
idempotency key, timestamp and payload. The timestamp window limits short-term
replay. A production adapter also needs a durable provider nonce or event ID.

Kafka is not exposed on the host in the local Compose topology. Production still
needs TLS/SASL, ACLs, separate PostgreSQL roles, network segmentation, rate
limits and managed secrets.

## Verification

| Layer | Coverage |
|---|---|
| Domain | aggregate transitions, invalid commands, replay and no-op rejection |
| Contract | envelope shape, schema versions, upcasting and canonical checks |
| Projection | idempotent receipts, stream gaps and canonical rebuild |
| PostgreSQL | idempotency races and the v1 → v2 → v3 projection path |
| Upgrade | legacy stream and outbox migration before worker delivery |
| Live | commands, outbox, Kafka, Settlement, refunds, rebuild and redrive |

Run the hermetic suite with `bazel test //...`. GitHub Actions runs the Compose
integration checks for adapters that cannot be proven by the SQLite path.

## Deliberate limits

- no capital reservation before bidding;
- no deadline scheduler or automatic lot reassignment;
- no inventory, shipments, units or multiple currencies;
- no Schema Registry, Prometheus, OpenTelemetry or Grafana;
- no production-grade tenant isolation;
- no snapshots.

These are explicit boundaries. They are the next vertical slices, not
placeholders for generic CRUD endpoints.
