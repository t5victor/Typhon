from dataclasses import dataclass


@dataclass(frozen=True)
class AcceptWinningBid:
    auction_id: str
