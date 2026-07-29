from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal

from thyphon.auction.domain.events.auction_expired.event import AuctionExpired
from thyphon.auction.domain.events.auction_opened.event import AuctionOpened
from thyphon.auction.domain.events.competitive_bid_placed.event import CompetitiveBidPlaced
from thyphon.auction.domain.events.winning_bid_accepted.event import WinningBidAccepted
from thyphon.shared.domain import DomainEvent, DomainViolation


@dataclass
class Auction:
    stream_id: str
    resource: str | None = None
    quantity: int = 0
    reserve_price: Decimal = Decimal("0")
    leading_company_id: str | None = None
    leading_offer: Decimal | None = None
    lifecycle: str = "unopened"
    version: int = 0
    _uncommitted: list[DomainEvent] = field(default_factory=list)

    @classmethod
    def rehydrate(cls, auction_id: str, history: list[DomainEvent]) -> Auction:
        auction = cls(stream_id=auction_id)
        for event in history:
            auction._apply(event)
            auction.version += 1
        return auction

    def open(self, resource: str, quantity: int, reserve_price: Decimal) -> None:
        if self.lifecycle != "unopened":
            raise DomainViolation("an auction can only be opened once")
        if not resource.strip() or quantity <= 0 or reserve_price <= 0:
            raise DomainViolation("an auction needs a resource, positive quantity and reserve")
        self._record(AuctionOpened.now(resource=resource, quantity=quantity, reserve_price=reserve_price))

    def place_competitive_bid(self, company_id: str, offer: Decimal) -> None:
        if self.lifecycle != "open":
            raise DomainViolation("only an open auction can receive a competitive bid")
        floor = self.leading_offer or self.reserve_price
        if offer <= floor:
            raise DomainViolation("a competitive bid must improve the current market offer")
        self._record(CompetitiveBidPlaced.now(company_id=company_id, offer=offer))

    def accept_winning_bid(self) -> None:
        if self.lifecycle != "open" or self.leading_company_id is None or self.leading_offer is None:
            raise DomainViolation("a winner can only be accepted after a valid competitive bid")
        self._record(
            WinningBidAccepted.now(
                company_id=self.leading_company_id, accepted_offer=self.leading_offer
            )
        )

    def expire(self, expired_at: datetime) -> None:
        if self.lifecycle != "open":
            raise DomainViolation("only an open auction can expire")
        self._record(AuctionExpired.now(expired_at=expired_at))

    def pull_uncommitted_events(self) -> list[DomainEvent]:
        events, self._uncommitted = self._uncommitted, []
        return events

    def _record(self, event: DomainEvent) -> None:
        self._apply(event)
        self._uncommitted.append(event)

    def _apply(self, event: DomainEvent) -> None:
        match event:
            case AuctionOpened(resource=resource, quantity=quantity, reserve_price=reserve_price):
                self.resource, self.quantity, self.reserve_price, self.lifecycle = (
                    resource, quantity, reserve_price, "open"
                )
            case CompetitiveBidPlaced(company_id=company_id, offer=offer):
                self.leading_company_id, self.leading_offer = company_id, offer
            case WinningBidAccepted():
                self.lifecycle = "allocated"
            case AuctionExpired():
                self.lifecycle = "expired"
            case _:
                raise TypeError(f"Auction cannot apply {type(event).__name__}")
