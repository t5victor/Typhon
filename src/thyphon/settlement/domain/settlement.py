from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from thyphon.settlement.domain.events.late_settlement_detected.event import LateSettlementDetected
from thyphon.settlement.domain.events.refund_completed.event import RefundCompleted
from thyphon.settlement.domain.events.refund_failed.event import RefundFailed
from thyphon.settlement.domain.events.refund_requested.event import RefundRequested
from thyphon.settlement.domain.events.settlement_confirmed.event import SettlementConfirmed
from thyphon.settlement.domain.events.settlement_rejected.event import SettlementRejected
from thyphon.settlement.domain.events.settlement_requested.event import SettlementRequested
from thyphon.shared.domain import DomainEvent, DomainViolation


@dataclass
class Settlement:
    stream_id: str
    auction_id: str | None = None
    payer_company_id: str | None = None
    winning_bid_event_id: str | None = None
    refund_provider_reference: str | None = None
    amount: Decimal = Decimal("0")
    lifecycle: str = "unrequested"
    version: int = 0
    _uncommitted: list[DomainEvent] = field(default_factory=list)

    @classmethod
    def rehydrate(cls, settlement_id: str, history: list[DomainEvent]) -> Settlement:
        settlement = cls(stream_id=settlement_id)
        for event in history:
            settlement._apply(event)
            settlement.version += 1
        return settlement

    def request(self, auction_id: str, payer_company_id: str, amount: Decimal, winning_bid_event_id: str) -> None:
        if self.lifecycle != "unrequested" or amount <= 0 or not winning_bid_event_id.strip():
            raise DomainViolation("a settlement request must name a new positive-value obligation and its winning bid")
        self._record(SettlementRequested.now(
            auction_id=auction_id, payer_company_id=payer_company_id, amount=amount, winning_bid_event_id=winning_bid_event_id,
        ))

    def confirm(self, provider_reference: str) -> None:
        if not provider_reference.strip():
            raise DomainViolation("a provider reference identifies every settlement result")
        if self.lifecycle == "requested":
            self._record(SettlementConfirmed.now(provider_reference=provider_reference))
        elif self.lifecycle == "rejected":
            self._record(LateSettlementDetected.now(provider_reference=provider_reference))
            self._record(RefundRequested.now(
                provider_reference=provider_reference, amount=self.amount,
                reason="settlement arrived after release of the winning claim",
            ))
            # A late confirmation creates exactly one compensation workflow and never revives the claim.
            self.lifecycle = "refund_pending"
        else:
            raise DomainViolation("only a requested or rejected settlement can receive confirmation")

    def complete_refund(self, provider_reference: str) -> None:
        if self.lifecycle != "refund_pending":
            raise DomainViolation("only a pending refund can be completed")
        if provider_reference != self.refund_provider_reference:
            raise DomainViolation("a refund outcome must name the provider reference that requested it")
        self._record(RefundCompleted.now(provider_reference=provider_reference))

    def fail_refund(self, provider_reference: str, failure_reason: str) -> None:
        if self.lifecycle != "refund_pending" or not failure_reason.strip():
            raise DomainViolation("only a pending refund can fail with a reason")
        if provider_reference != self.refund_provider_reference:
            raise DomainViolation("a refund outcome must name the provider reference that requested it")
        self._record(RefundFailed.now(
            provider_reference=provider_reference, failure_reason=failure_reason
        ))

    def reject(self, rejection_reason: str) -> None:
        if self.lifecycle != "requested" or not rejection_reason.strip():
            raise DomainViolation("only a requested settlement can be rejected with a reason")
        self._record(SettlementRejected.now(rejection_reason=rejection_reason))

    def pull_uncommitted_events(self) -> list[DomainEvent]:
        events, self._uncommitted = self._uncommitted, []
        return events

    def _record(self, event: DomainEvent) -> None:
        self._apply(event)
        self._uncommitted.append(event)

    def _apply(self, event: DomainEvent) -> None:
        match event:
            case SettlementRequested(auction_id=auction_id, payer_company_id=payer, amount=amount, winning_bid_event_id=winning_bid_event_id):
                self.winning_bid_event_id = winning_bid_event_id
                self.auction_id, self.payer_company_id, self.amount, self.lifecycle = auction_id, payer, amount, "requested"
            case SettlementConfirmed():
                self.lifecycle = "confirmed"
            case SettlementRejected():
                self.lifecycle = "rejected"
            case LateSettlementDetected():
                pass
            case RefundRequested(provider_reference=provider_reference):
                self.refund_provider_reference = provider_reference
                self.lifecycle = "refund_pending"
            case RefundCompleted():
                self.lifecycle = "refunded"
            case RefundFailed():
                self.lifecycle = "refund_failed"
            case _:
                raise TypeError(f"Settlement cannot apply {type(event).__name__}")
