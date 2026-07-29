from dataclasses import dataclass
from decimal import Decimal

from thyphon.shared.domain import DomainEvent


@dataclass(frozen=True, kw_only=True)
class AuctionOpened(DomainEvent):
    resource: str
    quantity: int
    reserve_price: Decimal
