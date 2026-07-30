from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from random import Random

from thyphon.auction.domain.auction import Auction
from thyphon.auction.domain.commands.open_auction.command import OpenAuction
from thyphon.auction.domain.commands.place_competitive_bid.command import PlaceCompetitiveBid
from thyphon.infrastructure.kafka_outbox_dispatcher import KafkaOutboxDispatcher
from thyphon.infrastructure.sqlite_event_store import SqliteEventStore
from thyphon.projections.auction_overview import AuctionOverviewProjector
from thyphon.shared.domain import CommandContext, DomainViolation, command_metadata, stream_key


@dataclass(frozen=True)
class MarketTick:
    index: int
    company_name: str
    offer: Decimal
    outcome: str
    market_note: str


@dataclass
class CompanyPulse:
    name: str
    strategy: str
    risk: Decimal
    cash: Decimal
    bids: int = 0
    last_action: str = "observing market"


class DeterministicMarket:
    _tape_limit = 240
    _published_limit = 64

    def __init__(self, seed: int) -> None:
        self.seed = seed
        self.store = SqliteEventStore()
        self.projector = AuctionOverviewProjector(self.store)
        self.tape: list[MarketTick] = []
        self._published: list[tuple[str, str, bytes]] = []
        self.published_count = 0
        self._projected_position = 0
        self.random = Random(seed)
        self.tick = 0
        self.started = False
        self.auction = Auction.rehydrate(stream_key("auction", "auction-lithium-381"), [])
        self.prices = {
            "Lithium": Decimal("212.00"),
            "Gold": Decimal("154.32"),
            "Copper": Decimal("23.12"),
            "Titanium": Decimal("88.90"),
        }
        self.opening_prices = dict(self.prices)
        self.price_history = {resource: [price] for resource, price in self.prices.items()}
        self.last_moves = {resource: Decimal("0") for resource in self.prices}
        self.companies = {
            "Astra Industries": CompanyPulse("Astra Industries", "AGGRESSIVE", Decimal("0.85"), Decimal("12.3")),
            "Helios Dynamics": CompanyPulse("Helios Dynamics", "MOMENTUM", Decimal("0.72"), Decimal("8.4")),
            "Nova Corp": CompanyPulse("Nova Corp", "LONG-TERM", Decimal("0.18"), Decimal("200.0")),
            "Blue Horizon": CompanyPulse("Blue Horizon", "VALUE", Decimal("0.41"), Decimal("42.8")),
        }

    def run(self, ticks: int) -> None:
        self.bootstrap()
        for _ in range(ticks):
            self.advance()

    def bootstrap(self) -> None:
        if self.started:
            return
        command = OpenAuction(
            auction_id="auction-lithium-381", resource="Lithium", quantity=1200,
            reserve_price=Decimal("212.00"),
        )
        expected_version = self.auction.version
        self.auction.open(command.resource, command.quantity, command.reserve_price)
        self._append_auction(command, expected_version, self._context("open"))
        self.started = True
        self._flush_events(duplicate_first=True)

    def advance(self) -> None:
        self.bootstrap()
        self.tick += 1
        self._move_prices()
        company = self._select_company()
        overview = self.projector.overview("auction-lithium-381")
        current_offer = Decimal(overview["leading_offer"] or overview["reserve_price"])
        offer = current_offer + Decimal(self.random.randint(1, 7))
        try:
            command = PlaceCompetitiveBid(
                auction_id="auction-lithium-381", company_id=company, offer=offer,
            )
            expected_version = self.auction.version
            self.auction.place_competitive_bid(command.company_id, command.offer)
            self._append_auction(command, expected_version, self._context(f"tick:{self.tick}"))
            outcome = "COMPETITIVE BID ACCEPTED"
        except DomainViolation as error:
            outcome = f"REJECTED: {error}"
        pulse = self.companies[company]
        pulse.bids += 1
        pulse.last_action = f"bid {offer:.2f} EUR on Lithium"
        pulse.cash = max(Decimal("0"), pulse.cash - (offer - current_offer) / Decimal("100"))
        note = self._market_note()
        self.tape.append(MarketTick(self.tick, company, offer, outcome, note))
        del self.tape[:-self._tape_limit]
        self._flush_events()

    def _append_auction(self, command: OpenAuction | PlaceCompetitiveBid, expected_version: int, context: CommandContext) -> None:
        command_name, request_hash = command_metadata(command)
        self.store.append(
            stream_id=self.auction.stream_id,
            expected_version=expected_version,
            events=self.auction.pull_uncommitted_events(),
            idempotency_key=context.idempotency_key,
            command_name=command_name,
            request_hash=request_hash,
            context=context,
        )

    def _flush_events(self, duplicate_first: bool = False) -> None:
        dispatcher = KafkaOutboxDispatcher(
            self.store, self._record_publication,
        )
        dispatcher.deliver_pending(duplicate_first=duplicate_first)
        # The projector is idempotent, but scanning the complete history every
        # tick is unnecessary and makes a long-running TUI quadratic.
        for event in self.store.events_after(self._projected_position):
            self.projector.apply(event)
            self._projected_position = event.global_position or self._projected_position

    def _record_publication(self, topic: str, key: str, body: bytes) -> None:
        self.published_count += 1
        self._published.append((topic, key, body))
        del self._published[:-self._published_limit]

    def _context(self, suffix: str) -> CommandContext:
        return CommandContext(
            idempotency_key=f"seed:{self.seed}:{suffix}", correlation_id=f"simulation:{self.seed}", actor_id="market-simulator",
        )

    def _select_company(self) -> str:
        """Strategy and appetite materially affect who enters the auction."""
        candidates: list[str] = []
        for name, pulse in self.companies.items():
            appetite = pulse.risk
            if pulse.strategy == "VALUE" and self.last_moves["Lithium"] > Decimal("1.0"):
                appetite /= Decimal("2")
            if pulse.strategy == "MOMENTUM" and self.last_moves["Lithium"] > 0:
                appetite += Decimal("0.15")
            if self.random.random() < float(min(Decimal("0.95"), appetite)):
                candidates.append(name)
        return candidates[self.random.randrange(len(candidates))] if candidates else "Nova Corp"

    def _move_prices(self) -> None:
        for resource, price in self.prices.items():
            basis_points = self.random.randint(-120, 180)
            move = Decimal(basis_points) / Decimal("100")
            self.last_moves[resource] = move
            self.prices[resource] = max(Decimal("0.01"), price * (Decimal("1") + move / Decimal("100"))).quantize(Decimal("0.01"))
            self.price_history[resource].append(self.prices[resource])
            self.price_history[resource] = self.price_history[resource][-36:]

    def _market_note(self) -> str:
        strongest = max(self.last_moves, key=lambda resource: abs(self.last_moves[resource]))
        move = self.last_moves[strongest]
        direction = "demand pulse" if move > 0 else "supply pressure"
        return f"{strongest} {direction} {move:+.2f}%"

    def price_change(self, resource: str) -> Decimal:
        return ((self.prices[resource] / self.opening_prices[resource]) - Decimal("1")) * Decimal("100")
