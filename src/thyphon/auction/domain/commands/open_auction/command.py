from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class OpenAuction:
    auction_id: str
    resource: str
    quantity: int
    reserve_price: Decimal
