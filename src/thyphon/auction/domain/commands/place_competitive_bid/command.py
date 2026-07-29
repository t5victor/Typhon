from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class PlaceCompetitiveBid:
    auction_id: str
    company_id: str
    offer: Decimal
    idempotency_key: str
    expected_version: int | None = None
