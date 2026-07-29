from decimal import Decimal
import unittest

from thyphon.application.auction_commands import AuctionCommandHandler
from thyphon.auction.domain.commands.open_auction.command import OpenAuction
from thyphon.auction.domain.commands.place_competitive_bid.command import PlaceCompetitiveBid
from thyphon.infrastructure.sqlite_event_store import SqliteEventStore
from thyphon.projections.auction_overview import AuctionOverviewProjector, ProjectionGap
from thyphon.shared.domain import CommandContext, stream_key


class AuctionOverviewOrdering(unittest.TestCase):
    def test_gap_is_not_receipted_and_can_be_applied_after_its_predecessor(self) -> None:
        store = SqliteEventStore()
        handler = AuctionCommandHandler(store)
        context = CommandContext("open", "projection-test", actor_id="supplier")
        handler.open_auction(OpenAuction("ordering", "Gold", 1, Decimal("100")), context)
        handler.place_competitive_bid(
            PlaceCompetitiveBid("ordering", "buyer", Decimal("105")),
            CommandContext("bid", "projection-test", actor_id="buyer"),
        )
        first, second = store.read_stream(stream_key("auction", "ordering"))
        projector = AuctionOverviewProjector(store)

        with self.assertRaises(ProjectionGap):
            projector.apply(second)
        self.assertTrue(projector.apply(first))
        self.assertTrue(projector.apply(second))

        overview = projector.overview("ordering")
        assert overview is not None
        self.assertEqual(overview["stream_version"], 2)
        self.assertEqual(overview["leading_company_id"], "buyer")

    def test_stream_rebuild_repairs_a_projection_from_canonical_order(self) -> None:
        store = SqliteEventStore()
        handler = AuctionCommandHandler(store)
        handler.open_auction(
            OpenAuction("repair", "Gold", 1, Decimal("100")), CommandContext("open", "repair", actor_id="supplier"),
        )
        handler.place_competitive_bid(
            PlaceCompetitiveBid("repair", "buyer", Decimal("105")), CommandContext("bid", "repair", actor_id="buyer"),
        )
        projector = AuctionOverviewProjector(store)
        self.assertEqual(projector.rebuild_stream(stream_key("auction", "repair")), 2)
        overview = projector.overview("repair")
        assert overview is not None
        self.assertEqual(overview["stream_version"], 2)
        self.assertEqual(overview["leading_offer"], "105")


if __name__ == "__main__":
    unittest.main()
