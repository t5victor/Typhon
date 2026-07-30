from __future__ import annotations

from thyphon.company.domain.commands.change_risk_appetite.command import ChangeRiskAppetite
from thyphon.company.domain.commands.onboard_company.command import OnboardCompany
from thyphon.company.domain.company import Company
from thyphon.shared.domain import CommandContext, EventStore, command_metadata, stream_key


class CompanyCommandHandler:
    def __init__(self, store: EventStore) -> None:
        self.store = store

    def onboard_company(self, command: OnboardCompany, context: CommandContext) -> int:
        command_name, request_hash = command_metadata(command)
        stream_id = stream_key("company", command.company_id)
        receipt = self.store.idempotency_result(context.idempotency_key, stream_id=stream_id, command_name=command_name, request_hash=request_hash, actor_id=context.actor_id, tenant_id=context.tenant_id)
        if receipt is not None:
            return receipt
        company = Company.rehydrate(stream_id, [])
        company.onboard(command.display_name, command.opening_capital, command.risk_appetite)
        return self.store.append(
            stream_id=stream_id, expected_version=0,
            events=company.pull_uncommitted_events(), idempotency_key=context.idempotency_key,
            command_name=command_name, request_hash=request_hash, context=context,
        )

    def change_risk_appetite(self, command: ChangeRiskAppetite, context: CommandContext) -> int:
        command_name, request_hash = command_metadata(command)
        stream_id = stream_key("company", command.company_id)
        receipt = self.store.idempotency_result(context.idempotency_key, stream_id=stream_id, command_name=command_name, request_hash=request_hash, actor_id=context.actor_id, tenant_id=context.tenant_id)
        if receipt is not None:
            return receipt
        stream = self.store.read_stream(stream_id)
        company = Company.rehydrate(stream_id, [item.event for item in stream])
        expected_version = company.version
        company.change_risk_appetite(command.new_appetite)
        return self.store.append(
            stream_id=stream_id, expected_version=expected_version,
            events=company.pull_uncommitted_events(), idempotency_key=context.idempotency_key,
            command_name=command_name, request_hash=request_hash, context=context,
        )
