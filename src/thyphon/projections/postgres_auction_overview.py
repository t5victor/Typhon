from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from thyphon.auction.domain.events.auction_expired.event import AuctionExpired
from thyphon.auction.domain.events.auction_opened.event import AuctionOpened
from thyphon.auction.domain.events.competitive_bid_placed.event import CompetitiveBidPlaced
from thyphon.auction.domain.events.winning_bid_accepted.event import WinningBidAccepted
from thyphon.infrastructure.sqlite_event_store import EVENT_TYPES, _decode_value
from thyphon.shared.domain import DomainEvent, RecordedEvent


class PostgresAuctionOverviewProjector:
    consumer_name = "auction-overview-v1"

    def __init__(self, connection_string: str) -> None:
        try:
            import psycopg
        except ImportError as error:  # pragma: no cover - runtime extra
            raise RuntimeError("Install Thyphon with the 'runtime' extra for PostgreSQL support") from error
        self.connection = psycopg.connect(
            connection_string, row_factory=psycopg.rows.dict_row, autocommit=True
        )

    @staticmethod
    def decode(envelope: dict[str, Any]) -> RecordedEvent:
        event_type = EVENT_TYPES[envelope["event_name"]]
        payload = envelope["payload"]
        event: DomainEvent = event_type(**{key: _decode_value(key, value) for key, value in payload.items()})
        return RecordedEvent(envelope["stream_id"], int(envelope["stream_version"]), event)

    def apply(self, recorded: RecordedEvent) -> bool:
        try:
            with self.connection.transaction(), self.connection.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO projection_receipt(consumer_name, event_id) VALUES (%s, %s)",
                    (self.consumer_name, recorded.event.event_id),
                )
                match recorded.event:
                    case AuctionOpened(resource=resource, quantity=quantity, reserve_price=reserve):
                        cursor.execute(
                            "INSERT INTO auction_overview VALUES (%s, %s, %s, %s, NULL, NULL, 'open', %s) "
                            "ON CONFLICT (auction_id) DO NOTHING",
                            (recorded.stream_id, resource, quantity, reserve, recorded.stream_version),
                        )
                    case CompetitiveBidPlaced(company_id=company, offer=offer):
                        cursor.execute(
                            "UPDATE auction_overview SET leading_company_id=%s, leading_offer=%s, stream_version=%s "
                            "WHERE auction_id=%s AND stream_version < %s",
                            (company, offer, recorded.stream_version, recorded.stream_id, recorded.stream_version),
                        )
                    case WinningBidAccepted():
                        cursor.execute(
                            "UPDATE auction_overview SET lifecycle='allocated', stream_version=%s "
                            "WHERE auction_id=%s AND stream_version < %s",
                            (recorded.stream_version, recorded.stream_id, recorded.stream_version),
                        )
                    case AuctionExpired():
                        cursor.execute(
                            "UPDATE auction_overview SET lifecycle='expired', stream_version=%s "
                            "WHERE auction_id=%s AND stream_version < %s",
                            (recorded.stream_version, recorded.stream_id, recorded.stream_version),
                        )
                    case _:
                        pass
            return True
        except Exception as error:
            if error.__class__.__name__ == "UniqueViolation":
                return False
            raise

    def overview(self, auction_id: str):
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT * FROM auction_overview WHERE auction_id=%s", (auction_id,))
            return cursor.fetchone()

    def rebuild(self) -> int:
        with self.connection.transaction(), self.connection.cursor() as cursor:
            cursor.execute("DELETE FROM auction_overview")
            cursor.execute("DELETE FROM projection_receipt WHERE consumer_name=%s", (self.consumer_name,))
            cursor.execute("SELECT stream_id, stream_version, event_name, payload FROM event_stream ORDER BY occurred_at, event_id")
            history = cursor.fetchall()
        for row in history:
            envelope = {
                "stream_id": row["stream_id"], "stream_version": row["stream_version"],
                "event_name": row["event_name"], "payload": row["payload"],
            }
            self.apply(self.decode(envelope))
        return len(history)
