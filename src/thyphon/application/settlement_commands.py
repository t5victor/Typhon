from __future__ import annotations

from thyphon.settlement.domain.commands.confirm_settlement.command import ConfirmSettlement
from thyphon.settlement.domain.commands.reject_settlement.command import RejectSettlement
from thyphon.settlement.domain.commands.request_settlement.command import RequestSettlement
from thyphon.settlement.domain.settlement import Settlement
from thyphon.shared.domain import EventStore


class SettlementCommandHandler:
    def __init__(self, store: EventStore) -> None:
        self.store = store

    def request_settlement(self, command: RequestSettlement) -> int:
        receipt = self.store.idempotency_result(command.idempotency_key)
        if receipt is not None:
            return receipt
        settlement = Settlement.rehydrate(command.settlement_id, [])
        settlement.request(command.auction_id, command.payer_company_id, command.amount)
        return self.store.append(
            stream_id=settlement.stream_id, expected_version=0,
            events=settlement.pull_uncommitted_events(), idempotency_key=command.idempotency_key,
        )

    def confirm_settlement(self, command: ConfirmSettlement) -> int:
        return self._execute(command.settlement_id, command.idempotency_key, lambda settlement: settlement.confirm(command.provider_reference))

    def reject_settlement(self, command: RejectSettlement) -> int:
        return self._execute(command.settlement_id, command.idempotency_key, lambda settlement: settlement.reject(command.rejection_reason))

    def _execute(self, settlement_id: str, idempotency_key: str, operation) -> int:
        receipt = self.store.idempotency_result(idempotency_key)
        if receipt is not None:
            return receipt
        stream = self.store.read_stream(settlement_id)
        settlement = Settlement.rehydrate(settlement_id, [item.event for item in stream])
        operation(settlement)
        return self.store.append(
            stream_id=settlement.stream_id, expected_version=settlement.version,
            events=settlement.pull_uncommitted_events(), idempotency_key=idempotency_key,
        )
