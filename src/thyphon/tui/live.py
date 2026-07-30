from __future__ import annotations

import curses
from decimal import Decimal
from time import monotonic, sleep

from thyphon.application.simulation import DeterministicMarket


class Colour:
    FRAME = 1
    TITLE = 2
    POSITIVE = 3
    NEGATIVE = 4
    AMBER = 5
    MUTED = 6
    ACCENT = 7


_colour_enabled = False


def _colour_setup() -> bool:
    if not curses.has_colors():
        return False
    curses.start_color()
    curses.use_default_colors()
    curses.init_pair(Colour.FRAME, curses.COLOR_CYAN, -1)
    curses.init_pair(Colour.TITLE, curses.COLOR_WHITE, curses.COLOR_BLUE)
    curses.init_pair(Colour.POSITIVE, curses.COLOR_GREEN, -1)
    curses.init_pair(Colour.NEGATIVE, curses.COLOR_RED, -1)
    curses.init_pair(Colour.AMBER, curses.COLOR_YELLOW, -1)
    curses.init_pair(Colour.MUTED, curses.COLOR_BLUE, -1)
    curses.init_pair(Colour.ACCENT, curses.COLOR_MAGENTA, -1)
    return True


def _add(screen, y: int, x: int, text: str, colour: int = 0, bold: bool = False) -> None:
    """Draw defensively: a resize or narrow terminal must not crash the console."""
    height, width = screen.getmaxyx()
    if y < 0 or y >= height or x >= width:
        return
    attributes = (curses.color_pair(colour) if _colour_enabled else 0) | (curses.A_BOLD if bold else 0)
    try:
        screen.addnstr(y, max(0, x), text, max(0, width - x - 1), attributes)
    except curses.error:
        pass


def _box(screen, top: int, left: int, height: int, width: int, title: str, colour: int) -> None:
    if height < 3 or width < 8:
        return
    horizontal = "-" * max(0, width - 2)
    _add(screen, top, left, "+" + horizontal + "+", colour)
    _add(screen, top, left + 2, f" {title} ", colour, True)
    for row in range(top + 1, top + height - 1):
        _add(screen, row, left, "|", colour)
        _add(screen, row, left + width - 1, "|", colour)
    _add(screen, top + height - 1, left, "+" + horizontal + "+", colour)


def _bar(value: Decimal, maximum: Decimal, width: int = 16) -> str:
    filled = max(0, min(width, int((value / maximum) * width))) if maximum else 0
    return "[" + "#" * filled + "." * (width - filled) + "]"


def _sparkline(values: list[Decimal], width: int = 22) -> str:
    values = values[-width:]
    if not values:
        return " " * width
    low, high = min(values), max(values)
    if high == low:
        return "-" * len(values)
    glyphs = ".:-=+*#%@"
    return "".join(glyphs[int((value - low) / (high - low) * (len(glyphs) - 1))] for value in values)


def _market_panel(screen, market: DeterministicMarket, top: int, left: int, width: int) -> None:
    _box(screen, top, left, 8, width, "MARKET PULSE", Colour.FRAME)
    for row, resource in enumerate(("Lithium", "Gold", "Copper", "Titanium"), start=1):
        change = market.price_change(resource)
        shade = Colour.POSITIVE if change >= 0 else Colour.NEGATIVE
        _add(screen, top + row, left + 2, f"{resource:<9} {market.prices[resource]:>7.2f} EUR", 0)
        _add(screen, top + row, left + 22, f"{change:+6.2f}%", shade, True)
        _add(screen, top + row, left + 30, _sparkline(market.price_history[resource], 18), Colour.MUTED)
    _add(screen, top + 6, left + 2, "Trend window: 36 ticks", Colour.MUTED)


def _auction_panel(screen, market: DeterministicMarket, top: int, left: int, width: int) -> None:
    _box(screen, top, left, 8, width, "LIVE AUCTION #381", Colour.POSITIVE)
    overview = market.projector.overview("auction-lithium-381")
    _add(screen, top + 1, left + 2, "LOT       Lithium / 1,200 units")
    _add(screen, top + 2, left + 2, f"RESERVE   {overview['reserve_price']:>10} EUR")
    _add(screen, top + 3, left + 2, f"LEADER    {(overview['leading_company_id'] or '-'):>16}", Colour.AMBER, True)
    _add(screen, top + 4, left + 2, f"OFFER     {(overview['leading_offer'] or '-'):>10} EUR", Colour.POSITIVE, True)
    _add(screen, top + 6, left + 2, "WRITE MODE: optimistic stream versioning", Colour.MUTED)


def _system_panel(screen, market: DeterministicMarket, top: int, left: int, width: int, interval: float, paused: bool) -> None:
    _box(screen, top, left, 8, width, "OPERATIONS", Colour.AMBER)
    events = market.store.event_count()
    rows = (
        ("SIMULATION", "PAUSED" if paused else "RUNNING", Colour.AMBER if paused else Colour.POSITIVE),
        ("TICK RATE", f"{1 / interval:4.1f}/sec", Colour.ACCENT),
        ("EVENTS", f"{events:>8}", Colour.FRAME),
        ("LOCAL OUTBOX", f"{market.published_count:>5} sent", Colour.POSITIVE),
        ("SIM PROJECTION", "caught up", Colour.POSITIVE),
    )
    for offset, (label, value, colour) in enumerate(rows, start=1):
        _add(screen, top + offset, left + 2, f"{label:<13}")
        _add(screen, top + offset, left + 16, value, colour, True)


def _companies_panel(screen, market: DeterministicMarket, top: int, left: int, width: int) -> None:
    height = 8
    _box(screen, top, left, height, width, "AUTONOMOUS COMPANIES", Colour.ACCENT)
    for offset, pulse in enumerate(market.companies.values(), start=1):
        _add(screen, top + offset, left + 2, f"{pulse.name[:17]:<17} {pulse.strategy:<10}")
        _add(screen, top + offset, left + 32, _bar(pulse.risk, Decimal("1"), 10), Colour.AMBER)
        _add(screen, top + offset, left + 45, f"{pulse.bids:>2} bids", Colour.FRAME)
    active = max(market.companies.values(), key=lambda item: item.bids)
    _add(screen, top + 6, left + 2, f"LATEST: {active.name} {active.last_action}", Colour.MUTED)


def _tape_panel(screen, market: DeterministicMarket, top: int, left: int, width: int, height: int) -> None:
    _box(screen, top, left, height, width, "SIMULATION TAPE :: DERIVED ACTIVITY", Colour.FRAME)
    capacity = max(1, height - 2)
    for offset, tick in enumerate(market.tape[-capacity:][::-1], start=1):
        _add(screen, top + offset, left + 2,
             f"{tick.index:04d}  {tick.company_name[:18]:<18} BID {tick.offer:>8.2f}  {tick.market_note}",
             Colour.POSITIVE if "ACCEPTED" in tick.outcome else Colour.NEGATIVE)


def _draw(screen, market: DeterministicMarket, interval: float, paused: bool) -> None:
    screen.erase()
    height, width = screen.getmaxyx()
    if width < 92 or height < 26:
        _add(screen, 1, 2, "Thyphon needs at least 92x26 characters. Resize this terminal.", Colour.AMBER, True)
        _add(screen, 3, 2, f"Current size: {width}x{height}. Press q to quit.", Colour.MUTED)
        screen.refresh()
        return
    title = f" THYPHON :: LIVE MARKET OPERATIONS :: SEED {market.seed:05d} :: TICK {market.tick:04d} "
    _add(screen, 0, 0, " " * (width - 1), Colour.TITLE)
    _add(screen, 0, max(1, (width - len(title)) // 2), title, Colour.TITLE, True)
    third = (width - 8) // 3
    _market_panel(screen, market, 2, 2, third)
    _auction_panel(screen, market, 2, 4 + third, third)
    _system_panel(screen, market, 2, 6 + 2 * third, width - (8 + 2 * third), interval, paused)
    _companies_panel(screen, market, 11, 2, width - 4)
    _tape_panel(screen, market, 20, 2, width - 4, height - 23)
    _add(screen, height - 2, 2, "[SPACE] pause/resume  [+/-] speed  [R] restart  [N] new seed  [Q] quit", Colour.FRAME, True)
    screen.refresh()


def _request_seed(screen, current_seed: int) -> int | None:
    """Read a new deterministic seed without leaving the curses console."""
    height, width = screen.getmaxyx()
    prompt = f"New seed (current {current_seed}, Enter cancels): "
    _add(screen, height - 2, 2, " " * max(0, width - 4), Colour.AMBER)
    _add(screen, height - 2, 2, prompt, Colour.AMBER, True)
    screen.refresh()
    try:
        curses.curs_set(1)
        curses.echo()
        screen.nodelay(False)
        raw = screen.getstr(height - 2, min(width - 2, len(prompt) + 2), 24).decode().strip()
    except curses.error:
        return None
    finally:
        curses.noecho()
        curses.curs_set(0)
        screen.nodelay(True)
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def run(screen, market: DeterministicMarket, ticks: int | None, interval: float, plain: bool = False) -> None:
    global _colour_enabled
    curses.curs_set(0)
    screen.nodelay(True)
    screen.keypad(True)
    _colour_enabled = not plain and _colour_setup()
    market.bootstrap()
    paused = False
    deadline = monotonic()
    completed = 0
    while ticks is None or completed < ticks:
        key = screen.getch()
        if key in (ord("q"), ord("Q")):
            return
        if key == ord(" "):
            paused = not paused
        elif key in (ord("+"), ord("=")):
            interval = max(0.04, interval * 0.75)
        elif key == ord("-"):
            interval = min(2.0, interval * 1.35)
        elif key in (ord("r"), ord("R")):
            market = DeterministicMarket(market.seed)
            market.bootstrap()
            completed = 0
        elif key in (ord("n"), ord("N")):
            requested_seed = _request_seed(screen, market.seed)
            if requested_seed is not None:
                market = DeterministicMarket(requested_seed)
                market.bootstrap()
                completed = 0
        now = monotonic()
        if not paused and now >= deadline:
            market.advance()
            completed += 1
            deadline = now + interval
        _draw(screen, market, interval, paused)
        sleep(0.02)
