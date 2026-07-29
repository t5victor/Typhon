"""Integration assertion for PostgreSQL idempotency-key serialization.

Run inside the API image so it exercises the same psycopg pool and event-store
adapter as the FastAPI command handlers.
"""
from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from threading import Barrier
from uuid import uuid4

from thyphon.application.auction_commands import AuctionCommandHandler
from thyphon.auction.domain.commands.open_auction.command import OpenAuction
from thyphon.auction.domain.commands.place_competitive_bid.command import PlaceCompetitiveBid
from thyphon.infrastructure.postgres_event_store import PostgresEventStore
from thyphon.shared.domain import CommandContext, IdempotencyKeyReused


def context(key: str, actor: str = "concurrency-bidder", tenant: str = "integration-tenant") -> CommandContext:
    return CommandContext(
        idempotency_key=key, correlation_id=f"integration:{key}", actor_id=actor, tenant_id=tenant,
    )


def main() -> None:
    store = PostgresEventStore(os.environ["THYPHON_DATABASE_URL"])
    handler = AuctionCommandHandler(store)
    run_id = uuid4().hex
    first_auction = f"concurrency-a-{run_id}"
    second_auction = f"concurrency-b-{run_id}"
    try:
        for auction_id in (first_auction, second_auction):
            handler.open_auction(
                OpenAuction(auction_id, "Copper", 1, Decimal("100.00")), context(f"open:{auction_id}", "supplier"),
            )

        retry_key = f"same-retry:{run_id}"
        barrier = Barrier(2)

        def identical_retry() -> int:
            barrier.wait()
            return handler.place_competitive_bid(
                PlaceCompetitiveBid(first_auction, "concurrency-bidder", Decimal("105.00")), context(retry_key),
            )

        with ThreadPoolExecutor(max_workers=2) as executor:
            identical_results = list(executor.map(lambda _: identical_retry(), range(2)))
        if identical_results != [2, 2]:
            raise AssertionError(f"identical concurrent retries must share their receipt, got {identical_results!r}")
        try:
            handler.place_competitive_bid(
                PlaceCompetitiveBid(first_auction, "concurrency-bidder", Decimal("105.00")),
                context(retry_key, "another-actor"),
            )
        except IdempotencyKeyReused:
            pass
        else:
            raise AssertionError("an idempotency receipt must not cross actor boundaries")
        try:
            handler.place_competitive_bid(
                PlaceCompetitiveBid(first_auction, "concurrency-bidder", Decimal("105.00")),
                context(retry_key, tenant="another-tenant"),
            )
        except IdempotencyKeyReused:
            pass
        else:
            raise AssertionError("an idempotency receipt must not cross tenant boundaries")

        collision_key = f"cross-stream:{run_id}"
        cross_stream_barrier = Barrier(2)

        def different_stream(auction_id: str, offer: Decimal) -> int:
            cross_stream_barrier.wait()
            return handler.place_competitive_bid(
                PlaceCompetitiveBid(auction_id, "concurrency-bidder", offer), context(collision_key),
            )

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(different_stream, first_auction, Decimal("110.00")),
                executor.submit(different_stream, second_auction, Decimal("105.00")),
            ]
            outcomes = []
            for future in futures:
                try:
                    outcomes.append(future.result())
                except IdempotencyKeyReused:
                    outcomes.append("idempotency-conflict")
        if sum(isinstance(outcome, int) for outcome in outcomes) != 1 or outcomes.count("idempotency-conflict") != 1:
            raise AssertionError(f"cross-stream key reuse must be a domain conflict, got {outcomes!r}")
    finally:
        store.close()


if __name__ == "__main__":
    main()
