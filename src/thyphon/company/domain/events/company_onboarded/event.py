from dataclasses import dataclass
from decimal import Decimal

from thyphon.shared.domain import DomainEvent


@dataclass(frozen=True, kw_only=True)
class CompanyOnboarded(DomainEvent):
    display_name: str
    opening_capital: Decimal
    risk_appetite: Decimal
