from __future__ import annotations

from thyphon.auction.domain.auction import Auction
from thyphon.auction.domain.commands.accept_winning_bid.command import AcceptWinningBid
from thyphon.auction.domain.commands.expire_auction.command import ExpireAuction
from thyphon.auction.domain.commands.open_auction.command import OpenAuction
from thyphon.auction.domain.commands.place_competitive_bid.command import PlaceCompetitiveBid
from thyphon.shared.domain import EventStore


class AuctionCommandHandler:
    def __init__(self, store: EventStore) -> None:
        self.store = store

    def open_auction(self, command: OpenAuction) -> int:
        receipt = self.store.idempotency_result(command.idempotency_key)
        if receipt is not None:
            return receipt
        auction = Auction.rehydrate(command.auction_id, [])
        auction.open(command.resource, command.quantity, command.reserve_price)
        return self.store.append(
            stream_id=auction.stream_id,
            expected_version=0,
            events=auction.pull_uncommitted_events(),
            idempotency_key=command.idempotency_key,
        )

    def place_competitive_bid(self, command: PlaceCompetitiveBid) -> int:
        receipt = self.store.idempotency_result(command.idempotency_key)
        if receipt is not None:
            return receipt
        stream = self.store.read_stream(command.auction_id)
        auction = Auction.rehydrate(command.auction_id, [item.event for item in stream])
        expected = auction.version if command.expected_version is None else command.expected_version
        auction.place_competitive_bid(command.company_id, command.offer)
        return self.store.append(
            stream_id=auction.stream_id,
            expected_version=expected,
            events=auction.pull_uncommitted_events(),
            idempotency_key=command.idempotency_key,
        )

    def accept_winning_bid(self, command: AcceptWinningBid) -> int:
        receipt = self.store.idempotency_result(command.idempotency_key)
        if receipt is not None:
            return receipt
        stream = self.store.read_stream(command.auction_id)
        auction = Auction.rehydrate(command.auction_id, [item.event for item in stream])
        auction.accept_winning_bid()
        return self.store.append(
            stream_id=auction.stream_id,
            expected_version=auction.version,
            events=auction.pull_uncommitted_events(),
            idempotency_key=command.idempotency_key,
        )

    def expire_auction(self, command: ExpireAuction) -> int:
        receipt = self.store.idempotency_result(command.idempotency_key)
        if receipt is not None:
            return receipt
        stream = self.store.read_stream(command.auction_id)
        auction = Auction.rehydrate(command.auction_id, [item.event for item in stream])
        auction.expire(command.expired_at)
        return self.store.append(
            stream_id=auction.stream_id,
            expected_version=auction.version,
            events=auction.pull_uncommitted_events(),
            idempotency_key=command.idempotency_key,
        )
