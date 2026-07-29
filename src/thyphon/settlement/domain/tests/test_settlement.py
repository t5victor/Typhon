from decimal import Decimal
import unittest

from thyphon.application.settlement_commands import SettlementCommandHandler
from thyphon.infrastructure.sqlite_event_store import SqliteEventStore
from thyphon.settlement.domain.commands.confirm_settlement.command import ConfirmSettlement
from thyphon.settlement.domain.commands.reject_settlement.command import RejectSettlement
from thyphon.settlement.domain.commands.request_settlement.command import RequestSettlement


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
        names = [record.event.event_name for record in store.read_stream("settlement-381")]
        self.assertEqual(names, [
            "SettlementRequested", "SettlementRejected", "LateSettlementDetected", "RefundRequested",
        ])


if __name__ == "__main__":
    unittest.main()
