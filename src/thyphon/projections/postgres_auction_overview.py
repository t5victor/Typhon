from __future__ import annotations

from typing import Any, cast

from thyphon.auction.domain.events.auction_expired.event import AuctionExpired
from thyphon.auction.domain.events.auction_opened.event import AuctionOpened
from thyphon.auction.domain.events.competitive_bid_placed.event import CompetitiveBidPlaced
from thyphon.auction.domain.events.winning_bid_accepted.event import WinningBidAccepted
from thyphon.infrastructure.sqlite_event_store import EVENT_TYPES, _decode_value, upcast_event
from thyphon.shared.domain import DomainEvent, RecordedEvent, aggregate_id


class PostgresAuctionOverviewProjector:
    consumer_name = "auction-overview-v1"

    def __init__(self, connection_string: str) -> None:
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError as error:  # pragma: no cover - runtime extra
            raise RuntimeError("Install Thyphon with the 'runtime' extra for PostgreSQL support") from error
        self.connection: Any = psycopg.connect(
            connection_string, row_factory=cast(Any, dict_row), autocommit=True
        )

    @staticmethod
    def decode(envelope: dict[str, Any]) -> RecordedEvent:
        event_type = EVENT_TYPES[envelope["event_name"]]
        schema_version, payload = upcast_event(envelope["event_name"], int(envelope.get("schema_version", 1)), envelope["payload"])
        event: DomainEvent = event_type(**{key: _decode_value(key, value) for key, value in payload.items()})
        return RecordedEvent(
            envelope["stream_id"], int(envelope["stream_version"]), event,
            int(envelope["global_position"]) if envelope.get("global_position") is not None else None, schema_version,
        )

    def apply(self, recorded: RecordedEvent) -> bool:
        try:
            with self.connection.transaction(), self.connection.cursor() as cursor:
                # Consumers and rebuilds serialize through the same advisory lock; no receipt can race a rebuild.
                cursor.execute("SELECT pg_advisory_xact_lock(421337)")
                cursor.execute(
                    "INSERT INTO projection_receipt(consumer_name, event_id) VALUES (%s, %s)",
                    (self.consumer_name, recorded.event.event_id),
                )
                if not recorded.stream_id.startswith("auction:"):
                    return True
                auction_id = aggregate_id(recorded.stream_id, "auction")
                match recorded.event:
                    case AuctionOpened(resource=resource, quantity=quantity, reserve_price=reserve):
                        cursor.execute(
                            "INSERT INTO auction_overview VALUES (%s, %s, %s, %s, NULL, NULL, 'open', %s) "
                            "ON CONFLICT (auction_id) DO NOTHING",
                            (auction_id, resource, quantity, reserve, recorded.stream_version),
                        )
                    case CompetitiveBidPlaced(company_id=company, offer=offer):
                        cursor.execute(
                            "UPDATE auction_overview SET leading_company_id=%s, leading_offer=%s, stream_version=%s "
                            "WHERE auction_id=%s AND stream_version < %s",
                            (company, offer, recorded.stream_version, auction_id, recorded.stream_version),
                        )
                    case WinningBidAccepted():
                        cursor.execute(
                            "UPDATE auction_overview SET lifecycle='allocated', stream_version=%s "
                            "WHERE auction_id=%s AND stream_version < %s",
                            (recorded.stream_version, auction_id, recorded.stream_version),
                        )
                    case AuctionExpired():
                        cursor.execute(
                            "UPDATE auction_overview SET lifecycle='expired', stream_version=%s "
                            "WHERE auction_id=%s AND stream_version < %s",
                            (recorded.stream_version, auction_id, recorded.stream_version),
                        )
                    case _:
                        pass
            return True
        except Exception as error:
            if error.__class__.__name__ == "UniqueViolation":
                return False
            raise

    def overview(self, auction_id: str) -> dict[str, Any] | None:
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT * FROM auction_overview WHERE auction_id=%s", (auction_id,))
            return cast(dict[str, Any] | None, cursor.fetchone())

    def rebuild(self) -> int:
        with self.connection.cursor() as lock_cursor:
            lock_cursor.execute("SELECT pg_advisory_lock(421337)")
        try:
            with self.connection.transaction(), self.connection.cursor() as cursor:
                cursor.execute("DELETE FROM auction_overview")
                cursor.execute("DELETE FROM projection_receipt WHERE consumer_name=%s", (self.consumer_name,))
                cursor.execute("SELECT stream_id, stream_version, event_name, payload, global_position, schema_version FROM event_stream WHERE stream_id LIKE 'auction:%' ORDER BY global_position")
                history = cursor.fetchall()
            for raw_row in history:
                row = cast(dict[str, Any], raw_row)
                envelope = {
                    "stream_id": row["stream_id"], "stream_version": row["stream_version"],
                    "event_name": row["event_name"], "payload": row["payload"],
                    "global_position": row["global_position"], "schema_version": row["schema_version"],
                }
                self.apply(self.decode(envelope))
            return len(history)
        finally:
            with self.connection.cursor() as lock_cursor:
                lock_cursor.execute("SELECT pg_advisory_unlock(421337)")
