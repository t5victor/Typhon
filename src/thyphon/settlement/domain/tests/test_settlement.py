from decimal import Decimal
import unittest

from thyphon.application.settlement_commands import SettlementCommandHandler
from thyphon.infrastructure.sqlite_event_store import SqliteEventStore
from thyphon.settlement.domain.commands.complete_refund.command import CompleteRefund
from thyphon.settlement.domain.commands.confirm_settlement.command import ConfirmSettlement
from thyphon.settlement.domain.commands.reject_settlement.command import RejectSettlement
from thyphon.settlement.domain.commands.request_settlement.command import RequestSettlement
from thyphon.settlement.domain.events.settlement_requested.event import SettlementRequested
from thyphon.shared.domain import CommandContext, DomainViolation, ProviderReferenceAlreadyObserved, stream_key


def delivery(key: str) -> CommandContext:
    return CommandContext(key, f"test:{key}", actor_id="payment-provider")


class SettlementBehaviour(unittest.TestCase):
    def test_late_confirmation_requests_one_compensation_and_can_be_completed_once(self) -> None:
        store = SqliteEventStore()
        handler = SettlementCommandHandler(store)
        handler.request_settlement(RequestSettlement("settlement-381", "auction-381", "helios", Decimal("219")), delivery("request"))
        handler.reject_settlement(RejectSettlement("settlement-381", "insufficient funds"), delivery("reject"))
        handler.confirm_settlement(ConfirmSettlement("settlement-381", "late-provider-381"), delivery("late"))
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
        handler = SettlementCommandHandler(SqliteEventStore())
        for settlement_id in ("settlement-a", "settlement-b"):
            handler.request_settlement(RequestSettlement(settlement_id, "auction-381", "helios", Decimal("219")), delivery(f"request-{settlement_id}"))
        handler.confirm_settlement(ConfirmSettlement("settlement-a", "provider-reference-unique"), delivery("confirm-a"))
        with self.assertRaises(ProviderReferenceAlreadyObserved):
            handler.confirm_settlement(ConfirmSettlement("settlement-b", "provider-reference-unique"), delivery("confirm-b"))


if __name__ == "__main__":
    unittest.main()
