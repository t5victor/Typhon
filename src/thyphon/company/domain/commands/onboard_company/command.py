from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class OnboardCompany:
    company_id: str
    display_name: str
    opening_capital: Decimal
    risk_appetite: Decimal
