from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class ChangeRiskAppetite:
    company_id: str
    new_appetite: Decimal
