"""Integration assertion for PostgreSQL projection v1 → v2 → v3.

This specifically exercises the dict-row adapter used by the live projection
worker. SQLite tests cannot detect mapping-versus-tuple access regressions.
"""
from __future__ import annotations

import os
from decimal import Decimal
from uuid import uuid4

from thyphon.application.auction_commands import AuctionCommandHandler
from thyphon.auction.domain.commands.accept_winning_bid.command import AcceptWinningBid
from thyphon.auction.domain.commands.open_auction.command import OpenAuction
from thyphon.auction.domain.commands.place_competitive_bid.command import PlaceCompetitiveBid
from thyphon.infrastructure.postgres_event_store import PostgresEventStore
from thyphon.projections.postgres_auction_overview import PostgresAuctionOverviewProjector
from thyphon.shared.domain import CommandContext, stream_key


def _context(key: str, actor: str) -> CommandContext:
    return CommandContext(key, f"postgres-projection:{key}", actor_id=actor, tenant_id="integration-tenant")


def main() -> None:
    store = PostgresEventStore(os.environ["THYPHON_DATABASE_URL"])
    projector = PostgresAuctionOverviewProjector(os.environ["THYPHON_DATABASE_URL"])
    auction_id = f"postgres-projection-{uuid4().hex}"
    handler = AuctionCommandHandler(store)
    try:
        handler.open_auction(
            OpenAuction(auction_id, "Copper", 4, Decimal("100.00")), _context(f"open:{auction_id}", "supplier"),
        )
        handler.place_competitive_bid(
            PlaceCompetitiveBid(auction_id, "buyer", Decimal("105.00")), _context(f"bid:{auction_id}", "buyer"),
        )
        handler.accept_winning_bid(AcceptWinningBid(auction_id), _context(f"accept:{auction_id}", "supplier"))
        for recorded in store.read_stream(stream_key("auction", auction_id)):
            projector.apply(recorded)
        overview = projector.overview(auction_id)
        if overview is None or overview["stream_version"] != 3:
            raise AssertionError(f"expected PostgreSQL projection at v3, got {overview!r}")
        if overview["leading_company_id"] != "buyer" or overview["lifecycle"] != "allocated":
            raise AssertionError(f"PostgreSQL projection did not apply bid and acceptance: {overview!r}")

        # A historic store may have populated delivery positions after the
        # fact. Rebuild must follow canonical stream versions rather than this
        # deliberately inverted delivery order.
        with store.connection() as connection, connection.transaction(), connection.cursor() as cursor:
            cursor.execute(
                "UPDATE event_stream SET global_position=%s WHERE stream_id=%s AND stream_version=%s",
                (1_000_003, f"auction:{auction_id}", 1),
            )
            cursor.execute(
                "UPDATE event_stream SET global_position=%s WHERE stream_id=%s AND stream_version=%s",
                (1_000_002, f"auction:{auction_id}", 2),
            )
            cursor.execute(
                "UPDATE event_stream SET global_position=%s WHERE stream_id=%s AND stream_version=%s",
                (1_000_001, f"auction:{auction_id}", 3),
            )
        projector.rebuild()
        rebuilt = projector.overview(auction_id)
        if rebuilt is None or rebuilt["stream_version"] != 3 or rebuilt["lifecycle"] != "allocated":
            raise AssertionError(f"rebuild accepted an inverted legacy delivery order: {rebuilt!r}")
    finally:
        projector.close()
        store.close()


if __name__ == "__main__":
    main()
