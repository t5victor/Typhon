from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class PlaceCompetitiveBid:
    auction_id: str
    company_id: str
    offer: Decimal
    expected_version: int | None = None
