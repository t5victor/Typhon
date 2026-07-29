from dataclasses import dataclass
from datetime import datetime

from thyphon.shared.domain import DomainEvent


@dataclass(frozen=True, kw_only=True)
class AuctionExpired(DomainEvent):
    expired_at: datetime
