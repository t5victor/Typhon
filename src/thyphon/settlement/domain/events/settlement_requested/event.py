from dataclasses import dataclass
from decimal import Decimal

from thyphon.shared.domain import DomainEvent


@dataclass(frozen=True, kw_only=True)
class SettlementRequested(DomainEvent):
    auction_id: str
    payer_company_id: str
    amount: Decimal
