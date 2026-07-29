from dataclasses import dataclass
from decimal import Decimal

from thyphon.shared.domain import DomainEvent


@dataclass(frozen=True, kw_only=True)
class RiskAppetiteChanged(DomainEvent):
    former_appetite: Decimal
    new_appetite: Decimal
