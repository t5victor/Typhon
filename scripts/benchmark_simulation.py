"""Deterministic, repeatable baseline for the terminal-market command path."""
from __future__ import annotations

import argparse
from time import perf_counter

from thyphon.application.simulation import DeterministicMarket


def main() -> None:
    parser = argparse.ArgumentParser(description="Measure deterministic Thyphon simulation throughput")
    parser.add_argument("--ticks", type=int, default=1_000)
    parser.add_argument("--seed", type=int, default=18_374)
    args = parser.parse_args()
    if args.ticks < 1:
        raise SystemExit("--ticks must be positive")
    market = DeterministicMarket(seed=args.seed)
    started = perf_counter()
    market.run(args.ticks)
    elapsed = perf_counter() - started
    events = len(market.store.all_events())
    print(
        f"seed={args.seed} ticks={args.ticks} events={events} elapsed_seconds={elapsed:.3f} "
        f"events_per_second={events / elapsed:.1f}"
    )


if __name__ == "__main__":
    main()
