from decimal import Decimal
import unittest

from thyphon.application.company_commands import CompanyCommandHandler
from thyphon.company.domain.commands.change_risk_appetite.command import ChangeRiskAppetite
from thyphon.company.domain.commands.onboard_company.command import OnboardCompany
from thyphon.infrastructure.sqlite_event_store import SqliteEventStore
from thyphon.shared.domain import DomainViolation


class CompanyBehaviour(unittest.TestCase):
    def test_policy_change_is_a_real_business_decision(self) -> None:
        handler = CompanyCommandHandler(SqliteEventStore())
        handler.onboard_company(OnboardCompany(
            company_id="helios", display_name="Helios Dynamics", opening_capital=Decimal("12000000"),
            risk_appetite=Decimal("0.85"), idempotency_key="onboard-helios",
        ))
        with self.assertRaisesRegex(DomainViolation, "real policy change"):
            handler.change_risk_appetite(ChangeRiskAppetite(
                company_id="helios", new_appetite=Decimal("0.85"), idempotency_key="no-change"
            ))


if __name__ == "__main__":
    unittest.main()
