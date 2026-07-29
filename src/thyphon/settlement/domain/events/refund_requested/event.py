from dataclasses import dataclass
from decimal import Decimal

from thyphon.shared.domain import DomainEvent


@dataclass(frozen=True, kw_only=True)
class RefundRequested(DomainEvent):
    provider_reference: str
    amount: Decimal
    reason: str
