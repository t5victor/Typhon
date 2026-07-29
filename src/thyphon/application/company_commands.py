from __future__ import annotations

from thyphon.company.domain.commands.change_risk_appetite.command import ChangeRiskAppetite
from thyphon.company.domain.commands.onboard_company.command import OnboardCompany
from thyphon.company.domain.company import Company
from thyphon.shared.domain import EventStore


class CompanyCommandHandler:
    def __init__(self, store: EventStore) -> None:
        self.store = store

    def onboard_company(self, command: OnboardCompany) -> int:
        receipt = self.store.idempotency_result(command.idempotency_key)
        if receipt is not None:
            return receipt
        company = Company.rehydrate(command.company_id, [])
        company.onboard(command.display_name, command.opening_capital, command.risk_appetite)
        return self.store.append(
            stream_id=company.stream_id, expected_version=0,
            events=company.pull_uncommitted_events(), idempotency_key=command.idempotency_key,
        )

    def change_risk_appetite(self, command: ChangeRiskAppetite) -> int:
        receipt = self.store.idempotency_result(command.idempotency_key)
        if receipt is not None:
            return receipt
        stream = self.store.read_stream(command.company_id)
        company = Company.rehydrate(command.company_id, [item.event for item in stream])
        company.change_risk_appetite(command.new_appetite)
        return self.store.append(
            stream_id=company.stream_id, expected_version=company.version,
            events=company.pull_uncommitted_events(), idempotency_key=command.idempotency_key,
        )
