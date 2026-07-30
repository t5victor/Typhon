import unittest

from thyphon.application.simulation import DeterministicMarket
from thyphon.tui.dashboard import render


class DashboardPresentation(unittest.TestCase):
    def test_plain_dashboard_is_ascii_readable_without_terminal_colour(self) -> None:
        market = DeterministicMarket(seed=18374)
        market.run(6)
        screen = render(market, width=96, colour=False)
        self.assertNotIn("\x1b[", screen)
        self.assertIn("THYPHON :: LOCAL MARKET SIMULATION", screen)
        self.assertIn("SIMULATION TAPE :: DERIVED ACTIVITY", screen)
        self.assertTrue(all(len(line) == 96 for line in screen.splitlines()))


if __name__ == "__main__":
    unittest.main()
