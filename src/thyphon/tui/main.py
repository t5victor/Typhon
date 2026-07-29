from __future__ import annotations

import argparse
import curses

from thyphon.application.simulation import DeterministicMarket
from thyphon.tui.dashboard import render
from thyphon.tui.live import run as run_live


def parser() -> argparse.ArgumentParser:
    parsed = argparse.ArgumentParser(prog="Thyphon", description="Thyphon ASCII market operations console")
    parsed.add_argument("--ticks", type=int, default=40)
    parsed.add_argument("--seed", type=int, default=18374)
    parsed.add_argument("--interval", type=float, default=0.25, help="seconds between live simulation ticks")
    parsed.add_argument("--snapshot", action="store_true", help="render once after all ticks instead of animating")
    parsed.add_argument("--plain", action="store_true", help="disable ANSI colour, preserving ASCII layout")
    return parsed


def main() -> None:
    args = parser().parse_args()
    market = DeterministicMarket(args.seed)
    if args.snapshot:
        market.run(args.ticks)
        print(render(market, colour=not args.plain))
        return
    try:
        curses.wrapper(run_live, market, args.ticks, max(0.02, args.interval))
    except KeyboardInterrupt:
        print("\nThyphon simulation stopped by operator.")


if __name__ == "__main__":
    main()
