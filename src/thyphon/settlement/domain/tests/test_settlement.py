from decimal import Decimal
import unittest
from uuid import uuid4

from thyphon.application.auction_commands import AuctionCommandHandler
from thyphon.application.settlement_commands import SettlementCommandHandler
from thyphon.auction.domain.commands.accept_winning_bid.command import AcceptWinningBid
from thyphon.auction.domain.commands.open_auction.command import OpenAuction
from thyphon.auction.domain.commands.place_competitive_bid.command import PlaceCompetitiveBid
from thyphon.infrastructure.sqlite_event_store import SqliteEventStore
from thyphon.settlement.domain.commands.complete_refund.command import CompleteRefund
from thyphon.settlement.domain.commands.confirm_settlement.command import ConfirmSettlement
from thyphon.settlement.domain.commands.fail_refund.command import FailRefund
from thyphon.settlement.domain.commands.reject_settlement.command import RejectSettlement
from thyphon.settlement.domain.commands.request_settlement.command import RequestSettlement
from thyphon.settlement.domain.events.settlement_requested.event import SettlementRequested
from thyphon.shared.domain import (
    CommandContext,
    DomainViolation,
    InvalidSettlementCausation,
    ProviderReferenceAlreadyObserved,
    SettlementAlreadyRequestedForWinningBid,
    stream_key,
)


def delivery(key: str) -> CommandContext:
    return CommandContext(key, f"test:{key}", actor_id="payment-provider")


def winning_bid_id(store: SqliteEventStore, auction_id: str, company_id: str = "helios"):
    auctions = AuctionCommandHandler(store)
    auctions.open_auction(
        OpenAuction(auction_id, "Gold", 1, Decimal("200")), CommandContext(f"open:{auction_id}", "settlement-test", actor_id="supplier"),
    )
    auctions.place_competitive_bid(
        PlaceCompetitiveBid(auction_id, company_id, Decimal("219")), CommandContext(f"bid:{auction_id}", "settlement-test", actor_id=company_id),
    )
    auctions.accept_winning_bid(
        AcceptWinningBid(auction_id), CommandContext(f"accept:{auction_id}", "settlement-test", actor_id="operator"),
    )
    return store.read_stream(f"auction:{auction_id}")[-1].event.event_id


class SettlementBehaviour(unittest.TestCase):
    def test_late_confirmation_requests_one_compensation_and_can_be_completed_once(self) -> None:
        store = SqliteEventStore()
        handler = SettlementCommandHandler(store)
        handler.request_settlement(RequestSettlement("settlement-381", "auction-381", "helios", Decimal("219"), winning_bid_id(store, "auction-381")), delivery("request"))
        handler.reject_settlement(RejectSettlement("settlement-381", "insufficient funds"), delivery("reject"))
        handler.confirm_settlement(ConfirmSettlement("settlement-381", "late-provider-381"), delivery("late"))
        with self.assertRaisesRegex(DomainViolation, "provider reference"):
            handler.fail_refund(FailRefund("settlement-381", "different-reference", "irrelevant"), delivery("refund-wrong-reference"))
        with self.assertRaisesRegex(DomainViolation, "provider reference"):
            handler.complete_refund(CompleteRefund("settlement-381", "different-reference"), delivery("refund-complete-wrong-reference"))
        handler.complete_refund(CompleteRefund("settlement-381", "late-provider-381"), delivery("refund-complete"))
        names = [record.event.event_name for record in store.read_stream(stream_key("settlement", "settlement-381"))]
        self.assertEqual(names, ["SettlementRequested", "SettlementRejected", "LateSettlementDetected", "RefundRequested", "RefundCompleted"])
        requested = store.read_stream(stream_key("settlement", "settlement-381"))[0].event
        self.assertIsInstance(requested, SettlementRequested)
        assert isinstance(requested, SettlementRequested)
        self.assertIsInstance(requested.amount, Decimal)
        with self.assertRaisesRegex(DomainViolation, "requested or rejected"):
            handler.confirm_settlement(ConfirmSettlement("settlement-381", "second-provider-reference"), delivery("second-late"))
        with self.assertRaisesRegex(DomainViolation, "pending"):
            handler.complete_refund(CompleteRefund("settlement-381", "late-provider-381"), delivery("refund-repeat"))

    def test_provider_reference_cannot_be_reused_for_another_settlement(self) -> None:
        store = SqliteEventStore()
        handler = SettlementCommandHandler(store)
        for settlement_id in ("settlement-a", "settlement-b"):
            auction_id = f"auction-{settlement_id}"
            winning_bid_event_id = winning_bid_id(store, auction_id)
            handler.request_settlement(RequestSettlement(settlement_id, auction_id, "helios", Decimal("219"), winning_bid_event_id), delivery(f"request-{settlement_id}"))
        handler.confirm_settlement(ConfirmSettlement("settlement-a", "provider-reference-unique"), delivery("confirm-a"))
        with self.assertRaises(ProviderReferenceAlreadyObserved):
            handler.confirm_settlement(ConfirmSettlement("settlement-b", "provider-reference-unique"), delivery("confirm-b"))

    def test_one_winning_bid_can_cause_only_one_settlement(self) -> None:
        store = SqliteEventStore()
        handler = SettlementCommandHandler(store)
        winning_bid_event_id = winning_bid_id(store, "auction-381")
        handler.request_settlement(
            RequestSettlement("settlement-a", "auction-381", "helios", Decimal("219"), winning_bid_event_id),
            delivery("request-a"),
        )
        with self.assertRaises(SettlementAlreadyRequestedForWinningBid):
            handler.request_settlement(
                RequestSettlement("settlement-b", "auction-381", "helios", Decimal("219"), winning_bid_event_id),
                delivery("request-b"),
            )

    def test_settlement_request_rejects_a_non_uuid_causal_identifier(self) -> None:
        handler = SettlementCommandHandler(SqliteEventStore())
        with self.assertRaisesRegex(DomainViolation, "winning bid"):
            handler.request_settlement(
                RequestSettlement("settlement-invalid", "auction-381", "helios", Decimal("219"), "not-a-uuid"),  # type: ignore[arg-type]
                delivery("invalid-cause"),
            )

    def test_settlement_request_requires_a_canonical_winning_bid(self) -> None:
        handler = SettlementCommandHandler(SqliteEventStore())
        with self.assertRaises(InvalidSettlementCausation):
            handler.request_settlement(
                RequestSettlement("settlement-forged", "auction-381", "helios", Decimal("219"), uuid4()),
                delivery("forged-cause"),
            )


if __name__ == "__main__":
    unittest.main()
