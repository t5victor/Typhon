from __future__ import annotations

from thyphon.settlement.domain.commands.confirm_settlement.command import ConfirmSettlement
from thyphon.settlement.domain.commands.complete_refund.command import CompleteRefund
from thyphon.settlement.domain.commands.fail_refund.command import FailRefund
from thyphon.settlement.domain.commands.reject_settlement.command import RejectSettlement
from thyphon.settlement.domain.commands.request_settlement.command import RequestSettlement
from thyphon.settlement.domain.settlement import Settlement
from thyphon.shared.domain import CommandContext, EventStore, command_metadata, stream_key


class SettlementCommandHandler:
    def __init__(self, store: EventStore) -> None:
        self.store = store

    def request_settlement(self, command: RequestSettlement, context: CommandContext) -> int:
        command_name, request_hash = command_metadata(command)
        stream_id = stream_key("settlement", command.settlement_id)
        receipt = self.store.idempotency_result(context.idempotency_key, stream_id=stream_id, command_name=command_name, request_hash=request_hash, actor_id=context.actor_id, tenant_id=context.tenant_id)
        if receipt is not None:
            return receipt
        settlement = Settlement.rehydrate(stream_id, [])
        settlement.request(command.auction_id, command.payer_company_id, command.amount, command.winning_bid_event_id)
        return self.store.append(
            stream_id=stream_id, expected_version=0,
            events=settlement.pull_uncommitted_events(), idempotency_key=context.idempotency_key,
            command_name=command_name, request_hash=request_hash, context=context,
        )

    def confirm_settlement(self, command: ConfirmSettlement, context: CommandContext) -> int:
        return self._execute(command, context, lambda settlement: settlement.confirm(command.provider_reference))

    def reject_settlement(self, command: RejectSettlement, context: CommandContext) -> int:
        return self._execute(command, context, lambda settlement: settlement.reject(command.rejection_reason))

    def complete_refund(self, command: CompleteRefund, context: CommandContext) -> int:
        return self._execute(command, context, lambda settlement: settlement.complete_refund(command.provider_reference))

    def fail_refund(self, command: FailRefund, context: CommandContext) -> int:
        return self._execute(command, context, lambda settlement: settlement.fail_refund(command.provider_reference, command.failure_reason))

    def _execute(self, command, context: CommandContext, operation) -> int:
        settlement_id = command.settlement_id
        stream_id = stream_key("settlement", settlement_id)
        idempotency_key = context.idempotency_key
        command_name, request_hash = command_metadata(command)
        receipt = self.store.idempotency_result(idempotency_key, stream_id=stream_id, command_name=command_name, request_hash=request_hash, actor_id=context.actor_id, tenant_id=context.tenant_id)
        if receipt is not None:
            return receipt
        stream = self.store.read_stream(stream_id)
        settlement = Settlement.rehydrate(stream_id, [item.event for item in stream])
        operation(settlement)
        return self.store.append(
            stream_id=stream_id, expected_version=settlement.version,
            events=settlement.pull_uncommitted_events(), idempotency_key=idempotency_key,
            command_name=command_name, request_hash=request_hash, context=context,
        )
