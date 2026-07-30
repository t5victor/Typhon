from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from thyphon.company.domain.events.company_onboarded.event import CompanyOnboarded
from thyphon.company.domain.events.risk_appetite_changed.event import RiskAppetiteChanged
from thyphon.shared.domain import DomainEvent, DomainViolation


@dataclass
class Company:
    stream_id: str
    display_name: str | None = None
    available_capital: Decimal = Decimal("0")
    risk_appetite: Decimal = Decimal("0")
    version: int = 0
    _uncommitted: list[DomainEvent] = field(default_factory=list)

    @classmethod
    def rehydrate(cls, company_id: str, history: list[DomainEvent]) -> Company:
        company = cls(stream_id=company_id)
        for event in history:
            company._apply(event)
            company.version += 1
        return company

    def onboard(self, display_name: str, opening_capital: Decimal, risk_appetite: Decimal) -> None:
        if self.display_name is not None:
            raise DomainViolation("a company can only be onboarded once")
        if not display_name.strip() or opening_capital <= 0 or not Decimal("0") <= risk_appetite <= Decimal("1"):
            raise DomainViolation("company policy requires positive capital and risk between zero and one")
        self._record(CompanyOnboarded.now(
            display_name=display_name, opening_capital=opening_capital, risk_appetite=risk_appetite
        ))

    def change_risk_appetite(self, new_appetite: Decimal) -> None:
        if self.display_name is None:
            raise DomainViolation("only an onboarded company can change its policy")
        if not Decimal("0") <= new_appetite <= Decimal("1"):
            raise DomainViolation("risk appetite must stay between zero and one")
        if new_appetite == self.risk_appetite:
            raise DomainViolation("risk appetite must represent a real policy change")
        self._record(RiskAppetiteChanged.now(
            former_appetite=self.risk_appetite, new_appetite=new_appetite
        ))

    def pull_uncommitted_events(self) -> list[DomainEvent]:
        events, self._uncommitted = self._uncommitted, []
        return events

    def _record(self, event: DomainEvent) -> None:
        self._apply(event)
        self._uncommitted.append(event)
        self.version += 1

    def _apply(self, event: DomainEvent) -> None:
        match event:
            case CompanyOnboarded(display_name=name, opening_capital=capital, risk_appetite=appetite):
                self.display_name, self.available_capital, self.risk_appetite = name, capital, appetite
            case RiskAppetiteChanged(new_appetite=appetite):
                self.risk_appetite = appetite
            case _:
                raise TypeError(f"Company cannot apply {type(event).__name__}")
