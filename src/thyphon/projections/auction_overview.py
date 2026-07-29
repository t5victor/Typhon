from __future__ import annotations

from thyphon.auction.domain.events.auction_expired.event import AuctionExpired
from thyphon.auction.domain.events.auction_opened.event import AuctionOpened
from thyphon.auction.domain.events.competitive_bid_placed.event import CompetitiveBidPlaced
from thyphon.auction.domain.events.winning_bid_accepted.event import WinningBidAccepted
from thyphon.infrastructure.sqlite_event_store import SqliteEventStore
from thyphon.shared.domain import RecordedEvent, aggregate_id


class ProjectionGap(RuntimeError):
    """A read model cannot safely skip a stream version."""


class AuctionOverviewProjector:
    consumer_name = "auction-overview-v1"

    def __init__(self, store: SqliteEventStore) -> None:
        self.store = store

    def apply(self, recorded: RecordedEvent) -> bool:
        """Returns false only for an already-observed, fully applied event."""
        with self.store.connection:
            receipt = self.store.connection.execute(
                "SELECT 1 FROM projection_receipt WHERE consumer_name=? AND event_id=?",
                (self.consumer_name, str(recorded.event.event_id)),
            ).fetchone()
            if receipt is not None:
                return False
            if not recorded.stream_id.startswith("auction:"):
                self.store.connection.execute(
                    "INSERT INTO projection_receipt VALUES (?, ?)",
                    (self.consumer_name, str(recorded.event.event_id)),
                )
                return True
            auction_id = aggregate_id(recorded.stream_id, "auction")
            match recorded.event:
                case AuctionOpened(resource=resource, quantity=quantity, reserve_price=reserve):
                    if recorded.stream_version != 1:
                        raise ProjectionGap(f"{recorded.stream_id} opened at version {recorded.stream_version}, expected 1")
                    self.store.connection.execute(
                        "INSERT INTO auction_overview VALUES (?, ?, ?, ?, NULL, NULL, 'open', ?)",
                        (auction_id, resource, quantity, str(reserve), recorded.stream_version),
                    )
                case CompetitiveBidPlaced(company_id=company, offer=offer):
                    self._require_next_version(auction_id, recorded)
                    update = self.store.connection.execute(
                        "UPDATE auction_overview SET leading_company_id=?, leading_offer=?, stream_version=? "
                        "WHERE auction_id=? AND stream_version=?",
                        (company, str(offer), recorded.stream_version, auction_id, recorded.stream_version - 1),
                    )
                    if update.rowcount != 1:
                        raise ProjectionGap(f"{recorded.stream_id} v{recorded.stream_version} did not change its projection")
                case WinningBidAccepted():
                    self._require_next_version(auction_id, recorded)
                    update = self.store.connection.execute(
                        "UPDATE auction_overview SET lifecycle='allocated', stream_version=? WHERE auction_id=? AND stream_version=?",
                        (recorded.stream_version, auction_id, recorded.stream_version - 1),
                    )
                    if update.rowcount != 1:
                        raise ProjectionGap(f"{recorded.stream_id} v{recorded.stream_version} did not change its projection")
                case AuctionExpired():
                    self._require_next_version(auction_id, recorded)
                    update = self.store.connection.execute(
                        "UPDATE auction_overview SET lifecycle='expired', stream_version=? WHERE auction_id=? AND stream_version=?",
                        (recorded.stream_version, auction_id, recorded.stream_version - 1),
                    )
                    if update.rowcount != 1:
                        raise ProjectionGap(f"{recorded.stream_id} v{recorded.stream_version} did not change its projection")
                case _:
                    raise TypeError(f"Auction overview cannot project {type(recorded.event).__name__}")
            self.store.connection.execute(
                "INSERT INTO projection_receipt VALUES (?, ?)",
                (self.consumer_name, str(recorded.event.event_id)),
            )
        return True

    def _require_next_version(self, auction_id: str, recorded: RecordedEvent) -> None:
        row = self.store.connection.execute(
            "SELECT stream_version FROM auction_overview WHERE auction_id=?", (auction_id,)
        ).fetchone()
        if row is None or int(row[0]) != recorded.stream_version - 1:
            observed = "missing" if row is None else str(row[0])
            raise ProjectionGap(f"{recorded.stream_id} v{recorded.stream_version} follows projected version {observed}")

    def rebuild(self) -> int:
        with self.store.connection:
            self.store.connection.execute("DELETE FROM auction_overview")
            self.store.connection.execute(
                "DELETE FROM projection_receipt WHERE consumer_name=?", (self.consumer_name,)
            )
        all_events = [event for event in self.store.all_events() if event.stream_id.startswith("auction:")]
        for recorded in all_events:
            self.apply(recorded)
        return len(all_events)

    def rebuild_stream(self, stream_id: str) -> int:
        if not stream_id.startswith("auction:"):
            return 0
        auction_id = aggregate_id(stream_id, "auction")
        history = self.store.read_stream(stream_id)
        with self.store.connection:
            self.store.connection.execute("DELETE FROM auction_overview WHERE auction_id=?", (auction_id,))
            self.store.connection.execute(
                "DELETE FROM projection_receipt WHERE consumer_name=? AND event_id IN "
                "(SELECT event_id FROM event_stream WHERE stream_id=?)",
                (self.consumer_name, stream_id),
            )
        for expected_version, recorded in enumerate(history, start=1):
            if recorded.stream_version != expected_version:
                raise ProjectionGap(f"{stream_id} has a canonical gap before v{expected_version}")
            self.apply(recorded)
        return len(history)

    def overview(self, auction_id: str):
        return self.store.connection.execute(
            "SELECT * FROM auction_overview WHERE auction_id=?", (auction_id,)
        ).fetchone()
