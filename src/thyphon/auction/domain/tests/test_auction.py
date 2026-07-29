from datetime import UTC, datetime
from decimal import Decimal
import unittest

from thyphon.application.auction_commands import AuctionCommandHandler
from thyphon.auction.domain.commands.expire_auction.command import ExpireAuction
from thyphon.auction.domain.commands.open_auction.command import OpenAuction
from thyphon.auction.domain.commands.place_competitive_bid.command import PlaceCompetitiveBid
from thyphon.infrastructure.sqlite_event_store import SqliteEventStore
from thyphon.projections.auction_overview import AuctionOverviewProjector
from thyphon.shared.domain import CommandContext, DomainViolation, IdempotencyKeyReused, OptimisticConcurrencyConflict, stream_key


def delivery(key: str) -> CommandContext:
    return CommandContext(key, f"test:{key}", actor_id="test-actor")


class AuctionBehaviour(unittest.TestCase):
    def setUp(self) -> None:
        self.store = SqliteEventStore()
        self.commands = AuctionCommandHandler(self.store)
        self.commands.open_auction(OpenAuction("a-381", "Lithium", 1200, Decimal("212.00")), delivery("open-a-381"))

    def test_competitive_bid_must_improve_the_market(self) -> None:
        self.commands.place_competitive_bid(PlaceCompetitiveBid("a-381", "astra", Decimal("213")), delivery("astra-1"))
        with self.assertRaisesRegex(DomainViolation, "improve"):
            self.commands.place_competitive_bid(PlaceCompetitiveBid("a-381", "helios", Decimal("213")), delivery("helios-1"))

    def test_only_one_writer_can_advance_a_stale_stream_version(self) -> None:
        self.commands.place_competitive_bid(PlaceCompetitiveBid("a-381", "astra", Decimal("213"), 1), delivery("astra-1"))
        with self.assertRaises(OptimisticConcurrencyConflict):
            self.commands.place_competitive_bid(PlaceCompetitiveBid("a-381", "helios", Decimal("214"), 1), delivery("helios-1"))

    def test_repeated_external_command_produces_one_business_fact(self) -> None:
        command = PlaceCompetitiveBid("a-381", "astra", Decimal("213"))
        self.assertEqual(self.commands.place_competitive_bid(command, delivery("retry-safe")), 2)
        self.assertEqual(self.commands.place_competitive_bid(command, delivery("retry-safe")), 2)
        self.assertEqual(len(self.store.read_stream(stream_key("auction", "a-381"))), 2)

    def test_reusing_an_idempotency_key_for_another_intention_is_rejected(self) -> None:
        with self.assertRaises(IdempotencyKeyReused):
            self.commands.open_auction(OpenAuction("a-382", "Gold", 10, Decimal("100")), delivery("open-a-381"))

    def test_projection_rebuild_equals_duplicate_safe_live_delivery(self) -> None:
        self.commands.place_competitive_bid(PlaceCompetitiveBid("a-381", "astra", Decimal("213")), delivery("astra-1"))
        projector = AuctionOverviewProjector(self.store)
        for event in self.store.all_events():
            projector.apply(event)
            projector.apply(event)
        live = dict(projector.overview("a-381"))
        projector.rebuild()
        self.assertEqual(dict(projector.overview("a-381")), live)

    def test_expired_auction_rejects_a_late_competitive_bid(self) -> None:
        self.commands.expire_auction(ExpireAuction("a-381", datetime.now(UTC)), delivery("expiry"))
        with self.assertRaisesRegex(DomainViolation, "open auction"):
            self.commands.place_competitive_bid(PlaceCompetitiveBid("a-381", "helios", Decimal("213")), delivery("late"))


if __name__ == "__main__":
    unittest.main()
