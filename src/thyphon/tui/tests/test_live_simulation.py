import unittest
from decimal import Decimal

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

    def test_long_session_bounds_presentation_buffers_without_replaying_history(self) -> None:
        market = DeterministicMarket(seed=18374)
        market.run(300)

        self.assertEqual(market.tick, 300)
        self.assertLessEqual(len(market.tape), market._tape_limit)
        self.assertLessEqual(len(market._published), market._published_limit)
        self.assertGreater(market.published_count, len(market._published))

    def test_market_path_is_seeded_non_explosive_and_downward_biased(self) -> None:
        downward_moves = 0
        upward_moves = 0
        for seed in range(20):
            market = DeterministicMarket(seed=seed)
            market.bootstrap()
            for _ in range(80):
                market.advance()
                for move in market.last_moves.values():
                    if move < 0:
                        downward_moves += 1
                    elif move > 0:
                        upward_moves += 1
            for resource, opening in market.opening_prices.items():
                self.assertLess(market.prices[resource], opening * Decimal("1.75"))
                self.assertGreater(market.prices[resource], opening * Decimal("0.15"))
        self.assertGreater(downward_moves, upward_moves)


if __name__ == "__main__":
    unittest.main()
