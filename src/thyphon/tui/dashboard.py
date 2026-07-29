from __future__ import annotations

from dataclasses import dataclass
import re
from shutil import get_terminal_size

from thyphon.application.simulation import DeterministicMarket


class Ink:
    """A restrained optional colour layer; borders and meaning remain readable without it."""

    reset = "\x1b[0m"
    cyan = "\x1b[36m"
    green = "\x1b[32m"
    amber = "\x1b[33m"
    red = "\x1b[31m"
    dim = "\x1b[2m"
    bold = "\x1b[1m"


@dataclass(frozen=True)
class Panel:
    title: str
    rows: tuple[str, ...]
    accent: str = Ink.cyan


def fit(text: str, width: int) -> str:
    plain = re.sub(r"\x1b\[[0-9;]*m", "", text)
    return plain[:width].ljust(width)


def panel(panel: Panel, width: int) -> list[str]:
    body_width = width - 2
    title = f" {panel.title} "
    top = "+" + title + "-" * max(0, body_width - len(title)) + "+"
    rows = ["|" + fit(row, body_width) + "|" for row in panel.rows]
    return [panel.accent + top + Ink.reset, *rows, panel.accent + "+" + "-" * body_width + "+" + Ink.reset]


def columns(panels: tuple[Panel, ...], total_width: int) -> list[str]:
    gutter = "  "
    available = total_width - len(gutter) * (len(panels) - 1)
    base, remainder = divmod(available, len(panels))
    widths = tuple(base + (1 if index < remainder else 0) for index in range(len(panels)))
    rendered = [panel(item, widths[index]) for index, item in enumerate(panels)]
    height = max(len(item) for item in rendered)
    return [
        gutter.join(items[index] if index < len(items) else " " * widths[position]
                    for position, items in enumerate(rendered))
        for index in range(height)
    ]


def render(market: DeterministicMarket, width: int | None = None, colour: bool = True) -> str:
    width = width or max(88, min(get_terminal_size((112, 30)).columns, 132))
    overview = market.projector.overview("auction-lithium-381")
    line = "=" * (width - 2)
    header = [
        Ink.cyan + "+" + line + "+" + Ink.reset,
        "|" + fit(
            f"{Ink.bold}THYPHON{Ink.reset} :: MARKET OPERATIONS".ljust(width - 38)
            + f"SEED {market.seed:05d} :: TICK {market.tick:04d}", width - 2
        ) + "|",
        Ink.cyan + "+" + line + "+" + Ink.reset,
    ]
    market_panel = Panel("MARKET PULSE", (
        f"Lithium   {market.prices['Lithium']:>8.2f} EUR {market.price_change('Lithium'):+6.2f}%",
        f"Gold      {market.prices['Gold']:>8.2f} EUR {market.price_change('Gold'):+6.2f}%",
        f"Copper    {market.prices['Copper']:>8.2f} EUR {market.price_change('Copper'):+6.2f}%",
        f"Titanium  {market.prices['Titanium']:>8.2f} EUR {market.price_change('Titanium'):+6.2f}%",
    ))
    auction_panel = Panel("LIVE AUCTION", (
        "#381  Lithium / 1,200 units",
        f"Reserve      {overview['reserve_price']:>12} EUR",
        f"Leader       {(overview['leading_company_id'] or '-'):>12}",
        f"Offer        {(overview['leading_offer'] or '-'):>12} EUR",
    ), Ink.green)
    health_panel = Panel("SYSTEM HEALTH", (
        "Runtime      local simulator",
        f"Events       {len(market.store.all_events()):>4}",
        f"Messages     {len(market._published):>4}",
        "Projection   idempotent",
    ), Ink.amber)
    tape_lines = [
        f" {tick.index:03d}  {tick.company_name[:18]:<18} {tick.outcome:<26} {tick.offer:>7.2f} EUR"
        for tick in market.tape[-6:][::-1]
    ]
    tape = panel(Panel("SIMULATION TAPE :: DERIVED ACTIVITY", tuple(tape_lines), Ink.cyan), width)
    footer = [
        Ink.cyan + "+" + line + "+" + Ink.reset,
        "|" + fit(" LIVE SIMULATION :: prices, agents and event stream update every tick  [Ctrl+C] stop", width - 2) + "|",
        Ink.cyan + "+" + line + "+" + Ink.reset,
    ]
    screen = "\n".join([*header, *columns((market_panel, auction_panel, health_panel), width), *tape, *footer])
    return screen if colour else re.sub(r"\x1b\[[0-9;]*m", "", screen)
