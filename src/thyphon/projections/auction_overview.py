from __future__ import annotations

import sqlite3

from thyphon.auction.domain.events.auction_expired.event import AuctionExpired
from thyphon.auction.domain.events.auction_opened.event import AuctionOpened
from thyphon.auction.domain.events.competitive_bid_placed.event import CompetitiveBidPlaced
from thyphon.auction.domain.events.winning_bid_accepted.event import WinningBidAccepted
from thyphon.infrastructure.sqlite_event_store import SqliteEventStore
from thyphon.shared.domain import RecordedEvent


class AuctionOverviewProjector:
    consumer_name = "auction-overview-v1"

    def __init__(self, store: SqliteEventStore) -> None:
        self.store = store

    def apply(self, recorded: RecordedEvent) -> bool:
        """Returns false for an already-observed event: at-least-once is safe here."""
        try:
            with self.store.connection:
                self.store.connection.execute(
                    "INSERT INTO projection_receipt VALUES (?, ?)",
                    (self.consumer_name, str(recorded.event.event_id)),
                )
                match recorded.event:
                    case AuctionOpened(resource=resource, quantity=quantity, reserve_price=reserve):
                        self.store.connection.execute(
                            "INSERT INTO auction_overview VALUES (?, ?, ?, ?, NULL, NULL, 'open', ?)",
                            (recorded.stream_id, resource, quantity, str(reserve), recorded.stream_version),
                        )
                    case CompetitiveBidPlaced(company_id=company, offer=offer):
                        self.store.connection.execute(
                            "UPDATE auction_overview SET leading_company_id=?, leading_offer=?, stream_version=? "
                            "WHERE auction_id=?",
                            (company, str(offer), recorded.stream_version, recorded.stream_id),
                        )
                    case WinningBidAccepted():
                        self.store.connection.execute(
                            "UPDATE auction_overview SET lifecycle='allocated', stream_version=? WHERE auction_id=?",
                            (recorded.stream_version, recorded.stream_id),
                        )
                    case AuctionExpired():
                        self.store.connection.execute(
                            "UPDATE auction_overview SET lifecycle='expired', stream_version=? WHERE auction_id=?",
                            (recorded.stream_version, recorded.stream_id),
                        )
                    case _:
                        pass
            return True
        except sqlite3.IntegrityError:
            return False

    def rebuild(self) -> int:
        with self.store.connection:
            self.store.connection.execute("DELETE FROM auction_overview")
            self.store.connection.execute(
                "DELETE FROM projection_receipt WHERE consumer_name=?", (self.consumer_name,)
            )
        all_events = self.store.all_events()
        for recorded in all_events:
            self.apply(recorded)
        return len(all_events)

    def overview(self, auction_id: str):
        return self.store.connection.execute(
            "SELECT * FROM auction_overview WHERE auction_id=?", (auction_id,)
        ).fetchone()
