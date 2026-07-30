# Thyphon event catalog

| Aggregate | Command | Event | Meaning |
|---|---|---|---|
| Auction | `OpenAuction` | `AuctionOpened` | A supplier exposed a finite lot to competition. |
| Auction | `PlaceCompetitiveBid` | `CompetitiveBidPlaced` | A company committed a strictly better offer. |
| Auction | `AcceptWinningBid` | `WinningBidAccepted` | The auctioneer selected the current leader. |
| Auction | `ExpireAuction` | `AuctionExpired` | The competition window elapsed without settlement. |
| Company | `OnboardCompany` | `CompanyOnboarded` | A participant entered the market with capital and policy. |
| Company | `ChangeRiskAppetite` | `RiskAppetiteChanged` | The company deliberately changed its bidding tolerance. |
| Settlement process manager | `RequestSettlement` | `SettlementRequested` | A winning company received a specific financial obligation, linked to the exact `WinningBidAccepted` event that caused it. |
| Settlement | `ConfirmSettlement` | `SettlementConfirmed` | A provider confirmed payment while the claim was still valid. |
| Settlement | `RejectSettlement` | `SettlementRejected` | A provider rejected a pending obligation with a reason. |
| Settlement | `ConfirmSettlement` after rejection | `LateSettlementDetected`, `RefundRequested` | Late money cannot silently restore a released claim. |
| Settlement | `CompleteRefund` / `FailRefund` | `RefundCompleted` / `RefundFailed` | Compensation is resolved exactly once and remains auditable. |

The catalog rejects mechanical CRUD titles. `Created`, `Updated`, `Deleted`, `SetStatus`, and `StatusChanged` are prohibited because they hide the business decision or fact being represented.

Event envelopes have a schema version and a global position. Schema versions below 1 or newer than the reader are rejected. `SettlementRequested` is currently v2; the reader upcasts v1 records by preserving the unknown causal event as `null`, while new `RequestSettlement` intentions require a UUID winning-bid ID. A winning-bid ID is globally claimed by one Settlement stream, is foreign-keyed to the immutable event log, and must identify the matching `WinningBidAccepted` fact for Auction, company and offer. Before a worker projects or starts a Settlement, it verifies that every envelope exactly matches its immutable Event Store row. Rebuild is an administrative command: `python -m thyphon.projections.rebuild`.

Quarantined projection events are inspected and redriven manually after their cause is fixed: `python -m thyphon.workers.redrive <event-id> [operator] [reason]`. That command records a durable, idempotent `attempt_id` with `pending`, `published`, `resolved`, `failed`, or `superseded` lifecycle state. The redrive-outbox worker later publishes it with the attempt in a Kafka header. The consumer locks and validates that exact attempt before rebuilding; receiving the record is sufficient even if the dispatcher has not yet persisted `published_at`. A duplicate delivery of a terminal attempt is an acknowledged no-op, but an unknown or mismatched attempt is quarantined. A redrive of an Auction stream rebuilds that stream from canonical Event Store order rather than applying one old event into a possible gap. The projection receipt is written only after the projection transition succeeds.

Quarantine itself is transactional. `projection_dead_letter_outbox` stores the publication intent with the source coordinates, a SHA-256, byte size and a bounded Base64 preview. The raw poison record, when it has no canonical event ID, remains only in `projection_raw_failure`; the DLQ never amplifies it by embedding the full envelope and Base64 copy.
