from decimal import Decimal
import unittest

from thyphon.application.simulation import DeterministicMarket


class LiveSimulationBehaviour(unittest.TestCase):
    def test_each_live_tick_changes_market_state_and_generates_agent_activity(self) -> None:
        market = DeterministicMarket(seed=18374)
        market.bootstrap()
        opening_lithium = market.prices["Lithium"]
        market.advance()
        market.advance()

        self.assertEqual(market.tick, 2)
        self.assertNotEqual(market.prices["Lithium"], opening_lithium)
        self.assertEqual(len(market.tape), 2)
        self.assertTrue(any(company.bids > 0 for company in market.companies.values()))
        self.assertTrue(all(tick.market_note for tick in market.tape))
        self.assertGreater(len(market.store.all_events()), 2)


if __name__ == "__main__":
    unittest.main()
