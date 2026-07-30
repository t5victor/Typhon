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

Avoid mechanical CRUD titles. `Created`, `Updated`, `Deleted`, `SetStatus` and
`StatusChanged` hide the decision or fact that matters to the domain.

Envelopes carry a schema version and global position. Readers reject versions
below 1 or newer than they understand. `SettlementRequested` is at v2; v1 is
upcast with an unknown causal event, while new `RequestSettlement` commands
require a UUID winning-bid ID. That ID is claimed by one Settlement stream,
foreign-keyed to the immutable log and checked against the matching Auction,
company and offer. Workers compare each broker envelope with its Event Store
record before projecting it or starting Settlement. Rebuild remains an
administrative command: `python -m thyphon.projections.rebuild`.

After fixing the cause, redrive with `python -m thyphon.workers.redrive
<event-id> [operator] [reason]`. The command records one durable `attempt_id`
with `pending`, `published`, `resolved`, `failed` or `superseded` state. The
redrive outbox adds the attempt to the Kafka headers. Consumers lock and verify
the attempt before rebuilding. A terminal duplicate is a no-op; an unknown or
mismatched attempt is quarantined. Auction redrive rebuilds the complete stream
in canonical order. Projection receipts are written after a real transition.

Quarantine is transactional. `projection_dead_letter_outbox` stores the source
coordinates, SHA-256, byte size and bounded Base64 preview. Raw non-canonical
records remain in `projection_raw_failure`; the DLQ never embeds a second full
copy.
