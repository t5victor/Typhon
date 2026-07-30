from __future__ import annotations

from typing import Any, cast

from thyphon.auction.domain.events.auction_expired.event import AuctionExpired
from thyphon.auction.domain.events.auction_opened.event import AuctionOpened
from thyphon.auction.domain.events.competitive_bid_placed.event import CompetitiveBidPlaced
from thyphon.auction.domain.events.winning_bid_accepted.event import WinningBidAccepted
from thyphon.infrastructure.sqlite_event_store import EVENT_TYPES, _decode_value, upcast_event
from thyphon.infrastructure.postgres_event_store import _require_envelope_shape
from thyphon.shared.domain import DomainEvent, RecordedEvent, aggregate_id


class ProjectionGap(RuntimeError):
    """The consumer received a stream version that cannot follow its read model."""


class PostgresAuctionOverviewProjector:
    consumer_name = "auction-overview-v1"

    def __init__(self, connection_string: str) -> None:
        try:
            from psycopg.rows import dict_row
            from psycopg_pool import ConnectionPool
        except ImportError as error:  # pragma: no cover - runtime extra
            raise RuntimeError("Install Thyphon with the 'runtime' extra for PostgreSQL support") from error
        self.pool: Any = ConnectionPool(
            conninfo=connection_string, min_size=1, max_size=12, kwargs={"row_factory": cast(Any, dict_row)}, open=True,
        )

    def close(self) -> None:
        self.pool.close()

    @staticmethod
    def decode(envelope: dict[str, Any]) -> RecordedEvent:
        envelope_event_id = _require_envelope_shape(envelope)
        event_type = EVENT_TYPES[envelope["event_name"]]
        schema_version, payload = upcast_event(envelope["event_name"], int(envelope.get("schema_version", 1)), envelope["payload"])
        event: DomainEvent = event_type(**{key: _decode_value(key, value) for key, value in payload.items()})
        if event.event_id != envelope_event_id:
            raise ValueError("domain-event envelope event_id does not match its decoded payload")
        return RecordedEvent(
            envelope["stream_id"], int(envelope["stream_version"]), event,
            int(envelope["global_position"]) if envelope.get("global_position") is not None else None, schema_version,
        )

    def apply(self, recorded: RecordedEvent) -> bool:
        try:
            with self.pool.connection() as connection, connection.transaction(), connection.cursor() as cursor:
                # Consumers and rebuilds serialize through the same advisory lock; no receipt can race a rebuild.
                cursor.execute("SELECT pg_advisory_xact_lock(421337)")
                cursor.execute(
                    "SELECT 1 FROM projection_receipt WHERE consumer_name=%s AND event_id=%s",
                    (self.consumer_name, recorded.event.event_id),
                )
                if cursor.fetchone() is not None:
                    return False
                if not recorded.stream_id.startswith("auction:"):
                    cursor.execute(
                        "INSERT INTO projection_receipt(consumer_name, event_id) VALUES (%s, %s)",
                        (self.consumer_name, recorded.event.event_id),
                    )
                    return True
                auction_id = aggregate_id(recorded.stream_id, "auction")
                match recorded.event:
                    case AuctionOpened(resource=resource, quantity=quantity, reserve_price=reserve):
                        if recorded.stream_version != 1:
                            raise ProjectionGap(f"{recorded.stream_id} opened at version {recorded.stream_version}, expected 1")
                        cursor.execute(
                            "INSERT INTO auction_overview VALUES (%s, %s, %s, %s, NULL, NULL, 'open', %s)",
                            (auction_id, resource, quantity, reserve, recorded.stream_version),
                        )
                    case CompetitiveBidPlaced(company_id=company, offer=offer):
                        self._require_next_version(cursor, auction_id, recorded)
                        cursor.execute(
                            "UPDATE auction_overview SET leading_company_id=%s, leading_offer=%s, stream_version=%s "
                            "WHERE auction_id=%s AND stream_version=%s",
                            (company, offer, recorded.stream_version, auction_id, recorded.stream_version - 1),
                        )
                    case WinningBidAccepted():
                        self._require_next_version(cursor, auction_id, recorded)
                        cursor.execute(
                            "UPDATE auction_overview SET lifecycle='allocated', stream_version=%s "
                            "WHERE auction_id=%s AND stream_version=%s",
                            (recorded.stream_version, auction_id, recorded.stream_version - 1),
                        )
                    case AuctionExpired():
                        self._require_next_version(cursor, auction_id, recorded)
                        cursor.execute(
                            "UPDATE auction_overview SET lifecycle='expired', stream_version=%s "
                            "WHERE auction_id=%s AND stream_version=%s",
                            (recorded.stream_version, auction_id, recorded.stream_version - 1),
                        )
                    case _:
                        pass
                if cursor.rowcount != 1:
                    raise ProjectionGap(f"{recorded.stream_id} v{recorded.stream_version} did not change its projection")
                cursor.execute(
                    "INSERT INTO projection_receipt(consumer_name, event_id) VALUES (%s, %s)",
                    (self.consumer_name, recorded.event.event_id),
                )
            return True
        except Exception as error:
            if error.__class__.__name__ == "UniqueViolation":
                raise ProjectionGap(f"{recorded.stream_id} v{recorded.stream_version} conflicts with its projection") from error
            raise

    @staticmethod
    def _require_next_version(cursor: Any, auction_id: str, recorded: RecordedEvent) -> None:
        cursor.execute("SELECT stream_version FROM auction_overview WHERE auction_id=%s FOR UPDATE", (auction_id,))
        current = cursor.fetchone()
        if current is None or int(current["stream_version"]) != recorded.stream_version - 1:
            observed = "missing" if current is None else str(current["stream_version"])
            raise ProjectionGap(f"{recorded.stream_id} v{recorded.stream_version} follows projected version {observed}")

    def overview(self, auction_id: str) -> dict[str, Any] | None:
        with self.pool.connection() as connection, connection.cursor() as cursor:
            cursor.execute("SELECT * FROM auction_overview WHERE auction_id=%s", (auction_id,))
            return cast(dict[str, Any] | None, cursor.fetchone())

    def rebuild(self) -> int:
        # Rebuild into a shadow table in one transaction. Readers see the old
        # projection until the final table swap, never an empty/partial view.
        with self.pool.connection() as connection, connection.transaction(), connection.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_xact_lock(421337)")
            cursor.execute("DROP TABLE IF EXISTS auction_overview_rebuild")
            cursor.execute("CREATE TABLE auction_overview_rebuild (LIKE auction_overview INCLUDING ALL)")
            cursor.execute(
                "SELECT event_id, stream_id, stream_version, event_name, payload, global_position, schema_version, "
                "occurred_at, correlation_id, causation_id, actor_id, tenant_id "
                "FROM event_stream WHERE stream_id LIKE 'auction:%' ORDER BY global_position"
            )
            history = cursor.fetchall()
            for raw_row in history:
                row = cast(dict[str, Any], raw_row)
                recorded = self.decode({
                    "event_id": str(row["event_id"]),
                    "stream_id": row["stream_id"], "stream_version": row["stream_version"],
                    "event_name": row["event_name"], "payload": row["payload"],
                    "global_position": row["global_position"], "schema_version": row["schema_version"],
                    "occurred_at": row["occurred_at"].isoformat(), "correlation_id": row["correlation_id"],
                    "causation_id": row["causation_id"], "actor_id": row["actor_id"], "tenant_id": row["tenant_id"],
                })
                self._apply_to(cursor, recorded, "auction_overview_rebuild")
            cursor.execute("ALTER TABLE auction_overview RENAME TO auction_overview_previous")
            cursor.execute("ALTER TABLE auction_overview_rebuild RENAME TO auction_overview")
            cursor.execute("DROP TABLE auction_overview_previous")
            cursor.execute("DELETE FROM projection_receipt WHERE consumer_name=%s", (self.consumer_name,))
            cursor.execute(
                "INSERT INTO projection_receipt(consumer_name, event_id) "
                "SELECT %s, event_id FROM event_stream WHERE stream_id LIKE 'auction:%'", (self.consumer_name,)
            )
            return len(history)

    def rebuild_stream(self, stream_id: str) -> int:
        """Repair one auction projection from canonical Event Store order."""
        if not stream_id.startswith("auction:"):
            return 0
        auction_id = aggregate_id(stream_id, "auction")
        with self.pool.connection() as connection, connection.transaction(), connection.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_xact_lock(421337)")
            cursor.execute(
                "SELECT event_id, stream_id, stream_version, event_name, payload, global_position, schema_version, "
                "occurred_at, correlation_id, causation_id, actor_id, tenant_id "
                "FROM event_stream WHERE stream_id=%s ORDER BY stream_version",
                (stream_id,),
            )
            history = cursor.fetchall()
            cursor.execute("DELETE FROM auction_overview WHERE auction_id=%s", (auction_id,))
            cursor.execute(
                "DELETE FROM projection_receipt WHERE consumer_name=%s "
                "AND event_id IN (SELECT event_id FROM event_stream WHERE stream_id=%s)",
                (self.consumer_name, stream_id),
            )
            expected_version = 1
            for raw_row in history:
                row = cast(dict[str, Any], raw_row)
                if int(row["stream_version"]) != expected_version:
                    raise ProjectionGap(f"{stream_id} has a canonical gap before v{expected_version}")
                expected_version += 1
                recorded = self.decode({
                    "event_id": str(row["event_id"]),
                    "stream_id": row["stream_id"], "stream_version": row["stream_version"],
                    "event_name": row["event_name"], "payload": row["payload"],
                    "global_position": row["global_position"], "schema_version": row["schema_version"],
                    "occurred_at": row["occurred_at"].isoformat(), "correlation_id": row["correlation_id"],
                    "causation_id": row["causation_id"], "actor_id": row["actor_id"], "tenant_id": row["tenant_id"],
                })
                self._apply_to(cursor, recorded, "auction_overview")
            cursor.execute(
                "INSERT INTO projection_receipt(consumer_name, event_id) "
                "SELECT %s, event_id FROM event_stream WHERE stream_id=%s",
                (self.consumer_name, stream_id),
            )
            return len(history)

    @staticmethod
    def _apply_to(cursor: Any, recorded: RecordedEvent, table: str) -> None:
        if not recorded.stream_id.startswith("auction:"):
            return
        auction_id = aggregate_id(recorded.stream_id, "auction")
        match recorded.event:
            case AuctionOpened(resource=resource, quantity=quantity, reserve_price=reserve):
                cursor.execute(
                    f"INSERT INTO {table} VALUES (%s, %s, %s, %s, NULL, NULL, 'open', %s) ON CONFLICT (auction_id) DO NOTHING",
                    (auction_id, resource, quantity, reserve, recorded.stream_version),
                )
            case CompetitiveBidPlaced(company_id=company, offer=offer):
                cursor.execute(
                    f"UPDATE {table} SET leading_company_id=%s, leading_offer=%s, stream_version=%s WHERE auction_id=%s AND stream_version < %s",
                    (company, offer, recorded.stream_version, auction_id, recorded.stream_version),
                )
            case WinningBidAccepted():
                cursor.execute(
                    f"UPDATE {table} SET lifecycle='allocated', stream_version=%s WHERE auction_id=%s AND stream_version < %s",
                    (recorded.stream_version, auction_id, recorded.stream_version),
                )
            case AuctionExpired():
                cursor.execute(
                    f"UPDATE {table} SET lifecycle='expired', stream_version=%s WHERE auction_id=%s AND stream_version < %s",
                    (recorded.stream_version, auction_id, recorded.stream_version),
                )
