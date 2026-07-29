from dataclasses import dataclass
from decimal import Decimal

from thyphon.shared.domain import DomainEvent


@dataclass(frozen=True, kw_only=True)
class WinningBidAccepted(DomainEvent):
    company_id: str
    accepted_offer: Decimal
