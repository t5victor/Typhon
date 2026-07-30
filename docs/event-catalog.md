# Event catalog

| Aggregate | Command | Event | Meaning |
|---|---|---|---|
| Auction | `OpenAuction` | `AuctionOpened` | A finite lot enters the market. |
| Auction | `PlaceCompetitiveBid` | `CompetitiveBidPlaced` | A company takes the lead with a better offer. |
| Auction | `AcceptWinningBid` | `WinningBidAccepted` | The operator allocates the lot to the leader. |
| Auction | `ExpireAuction` | `AuctionExpired` | The allocation window closes without a winner. |
| Company | `OnboardCompany` | `CompanyOnboarded` | A company joins the market. |
| Company | `ChangeRiskAppetite` | `RiskAppetiteChanged` | A company changes its bidding tolerance. |
| Settlement | `RequestSettlement` | `SettlementRequested` | The winning bid creates a payment obligation. |
| Settlement | `ConfirmSettlement` | `SettlementConfirmed` | The provider confirms the pending obligation. |
| Settlement | `RejectSettlement` | `SettlementRejected` | The provider rejects the obligation. |
| Settlement | late confirmation | `LateSettlementDetected`, `RefundRequested` | Rejected money enters compensation. |
| Settlement | `CompleteRefund`, `FailRefund` | `RefundCompleted`, `RefundFailed` | The compensation reaches a terminal result. |

## Naming

Commands name decisions. Events name facts. Avoid mechanical names such as
`Created`, `Updated`, `Deleted`, `SetStatus` and `StatusChanged`.

## Contract

Each envelope carries its event ID, stream ID and version, global position,
schema version, payload and trace metadata. Consumers reject malformed or
unsupported envelopes and verify every accepted Kafka record against its
PostgreSQL event before projecting it.

`SettlementRequested` is at schema v2. The reader can upcast v1 records with an
unknown winning-bid ID; new commands require the causal UUID. The ID is claimed
once and must match the Auction, company and offer stored in
`WinningBidAccepted`.

## Redrive

`python -m thyphon.workers.redrive <event-id> [operator] [reason]` queues one
durable repair attempt. The active attempt is reused on a repeated request.
Auction repair rebuilds the complete stream in canonical order. Terminal Kafka
duplicates are no-ops; unknown or mismatched attempts are quarantined.

For the full model, see [technical architecture](technical-architecture.md).
