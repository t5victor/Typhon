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

Event envelopes have a schema version and a global position. `SettlementRequested` is currently v2; the reader upcasts v1 records by preserving the unknown causal event as `null`. Rebuild is an administrative command: `python -m thyphon.projections.rebuild`.
