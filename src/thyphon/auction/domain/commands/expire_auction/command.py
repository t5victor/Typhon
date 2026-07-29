from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class ExpireAuction:
    auction_id: str
    expired_at: datetime
    idempotency_key: str
