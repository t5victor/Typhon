from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class RequestSettlement:
    settlement_id: str
    auction_id: str
    payer_company_id: str
    amount: Decimal
    idempotency_key: str
