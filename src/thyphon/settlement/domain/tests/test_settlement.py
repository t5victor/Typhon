from decimal import Decimal
import unittest

from thyphon.application.settlement_commands import SettlementCommandHandler
from thyphon.infrastructure.sqlite_event_store import SqliteEventStore
from thyphon.settlement.domain.commands.confirm_settlement.command import ConfirmSettlement
from thyphon.settlement.domain.commands.reject_settlement.command import RejectSettlement
from thyphon.settlement.domain.commands.request_settlement.command import RequestSettlement
from thyphon.shared.domain import DomainViolation, ProviderReferenceAlreadyObserved, stream_key


class SettlementBehaviour(unittest.TestCase):
    def test_late_confirmation_requests_compensation_instead_of_confirming_released_claim(self) -> None:
        store = SqliteEventStore()
        handler = SettlementCommandHandler(store)
        handler.request_settlement(RequestSettlement(
            settlement_id="settlement-381", auction_id="auction-381", payer_company_id="helios",
            amount=Decimal("219"), idempotency_key="request",
        ))
        handler.reject_settlement(RejectSettlement(
            settlement_id="settlement-381", rejection_reason="insufficient funds", idempotency_key="reject",
        ))
        handler.confirm_settlement(ConfirmSettlement(
            settlement_id="settlement-381", provider_reference="late-provider-381", idempotency_key="late",
        ))
        names = [record.event.event_name for record in store.read_stream(stream_key("settlement", "settlement-381"))]
        self.assertEqual(names, [
            "SettlementRequested", "SettlementRejected", "LateSettlementDetected", "RefundRequested",
        ])
        self.assertIsInstance(store.read_stream(stream_key("settlement", "settlement-381"))[0].event.amount, Decimal)

        with self.assertRaisesRegex(DomainViolation, "requested or rejected"):
            handler.confirm_settlement(ConfirmSettlement(
                settlement_id="settlement-381", provider_reference="second-provider-reference", idempotency_key="second-late"
            ))

    def test_provider_reference_cannot_be_reused_for_another_settlement(self) -> None:
        handler = SettlementCommandHandler(SqliteEventStore())
        for settlement_id in ("settlement-a", "settlement-b"):
            handler.request_settlement(RequestSettlement(
                settlement_id=settlement_id, auction_id="auction-381", payer_company_id="helios",
                amount=Decimal("219"), idempotency_key=f"request-{settlement_id}",
            ))
        handler.confirm_settlement(ConfirmSettlement(
            settlement_id="settlement-a", provider_reference="provider-reference-unique", idempotency_key="confirm-a",
        ))
        with self.assertRaises(ProviderReferenceAlreadyObserved):
            handler.confirm_settlement(ConfirmSettlement(
                settlement_id="settlement-b", provider_reference="provider-reference-unique", idempotency_key="confirm-b",
            ))


if __name__ == "__main__":
    unittest.main()
