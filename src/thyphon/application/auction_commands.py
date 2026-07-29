from __future__ import annotations

from thyphon.auction.domain.auction import Auction
from thyphon.auction.domain.commands.accept_winning_bid.command import AcceptWinningBid
from thyphon.auction.domain.commands.expire_auction.command import ExpireAuction
from thyphon.auction.domain.commands.open_auction.command import OpenAuction
from thyphon.auction.domain.commands.place_competitive_bid.command import PlaceCompetitiveBid
from thyphon.shared.domain import CommandContext, EventStore, command_metadata, stream_key


class AuctionCommandHandler:
    def __init__(self, store: EventStore) -> None:
        self.store = store

    def open_auction(self, command: OpenAuction, context: CommandContext) -> int:
        command_name, request_hash = command_metadata(command)
        stream_id = stream_key("auction", command.auction_id)
        receipt = self.store.idempotency_result(context.idempotency_key, stream_id=stream_id, command_name=command_name, request_hash=request_hash)
        if receipt is not None:
            return receipt
        auction = Auction.rehydrate(stream_id, [])
        auction.open(command.resource, command.quantity, command.reserve_price)
        return self.store.append(
            stream_id=stream_id,
            expected_version=0,
            events=auction.pull_uncommitted_events(),
            idempotency_key=context.idempotency_key,
            command_name=command_name, request_hash=request_hash, context=context,
        )

    def place_competitive_bid(self, command: PlaceCompetitiveBid, context: CommandContext) -> int:
        command_name, request_hash = command_metadata(command)
        stream_id = stream_key("auction", command.auction_id)
        receipt = self.store.idempotency_result(context.idempotency_key, stream_id=stream_id, command_name=command_name, request_hash=request_hash)
        if receipt is not None:
            return receipt
        stream = self.store.read_stream(stream_id)
        auction = Auction.rehydrate(stream_id, [item.event for item in stream])
        expected = auction.version if command.expected_version is None else command.expected_version
        auction.place_competitive_bid(command.company_id, command.offer)
        return self.store.append(
            stream_id=stream_id,
            expected_version=expected,
            events=auction.pull_uncommitted_events(),
            idempotency_key=context.idempotency_key,
            command_name=command_name, request_hash=request_hash, context=context,
        )

    def accept_winning_bid(self, command: AcceptWinningBid, context: CommandContext) -> int:
        command_name, request_hash = command_metadata(command)
        stream_id = stream_key("auction", command.auction_id)
        receipt = self.store.idempotency_result(context.idempotency_key, stream_id=stream_id, command_name=command_name, request_hash=request_hash)
        if receipt is not None:
            return receipt
        stream = self.store.read_stream(stream_id)
        auction = Auction.rehydrate(stream_id, [item.event for item in stream])
        auction.accept_winning_bid()
        return self.store.append(
            stream_id=stream_id,
            expected_version=auction.version,
            events=auction.pull_uncommitted_events(),
            idempotency_key=context.idempotency_key,
            command_name=command_name, request_hash=request_hash, context=context,
        )

    def expire_auction(self, command: ExpireAuction, context: CommandContext) -> int:
        command_name, request_hash = command_metadata(command)
        stream_id = stream_key("auction", command.auction_id)
        receipt = self.store.idempotency_result(context.idempotency_key, stream_id=stream_id, command_name=command_name, request_hash=request_hash)
        if receipt is not None:
            return receipt
        stream = self.store.read_stream(stream_id)
        auction = Auction.rehydrate(stream_id, [item.event for item in stream])
        auction.expire(command.expired_at)
        return self.store.append(
            stream_id=stream_id,
            expected_version=auction.version,
            events=auction.pull_uncommitted_events(),
            idempotency_key=context.idempotency_key,
            command_name=command_name, request_hash=request_hash, context=context,
        )
