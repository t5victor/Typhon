from decimal import Decimal
import unittest

from thyphon.application.company_commands import CompanyCommandHandler
from thyphon.company.domain.commands.change_risk_appetite.command import ChangeRiskAppetite
from thyphon.company.domain.commands.onboard_company.command import OnboardCompany
from thyphon.infrastructure.sqlite_event_store import SqliteEventStore
from thyphon.shared.domain import CommandContext, DomainViolation


class CompanyBehaviour(unittest.TestCase):
    def test_policy_change_is_a_real_business_decision(self) -> None:
        handler = CompanyCommandHandler(SqliteEventStore())
        handler.onboard_company(OnboardCompany("helios", "Helios Dynamics", Decimal("12000000"), Decimal("0.85")), CommandContext("onboard-helios", "test:onboard"))
        with self.assertRaisesRegex(DomainViolation, "real policy change"):
            handler.change_risk_appetite(ChangeRiskAppetite("helios", Decimal("0.85")), CommandContext("no-change", "test:no-change"))


if __name__ == "__main__":
    unittest.main()
