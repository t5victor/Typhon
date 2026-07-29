from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID

from thyphon.auction.domain.events.auction_expired.event import AuctionExpired
from thyphon.auction.domain.events.auction_opened.event import AuctionOpened
from thyphon.auction.domain.events.competitive_bid_placed.event import CompetitiveBidPlaced
from thyphon.auction.domain.events.winning_bid_accepted.event import WinningBidAccepted
from thyphon.company.domain.events.company_onboarded.event import CompanyOnboarded
from thyphon.company.domain.events.risk_appetite_changed.event import RiskAppetiteChanged
from thyphon.settlement.domain.events.late_settlement_detected.event import LateSettlementDetected
from thyphon.settlement.domain.events.refund_requested.event import RefundRequested
from thyphon.settlement.domain.events.refund_completed.event import RefundCompleted
from thyphon.settlement.domain.events.refund_failed.event import RefundFailed
from thyphon.settlement.domain.events.settlement_confirmed.event import SettlementConfirmed
from thyphon.settlement.domain.events.settlement_rejected.event import SettlementRejected
from thyphon.settlement.domain.events.settlement_requested.event import SettlementRequested
from thyphon.shared.domain import CommandContext, DomainEvent, IdempotencyKeyReused, OptimisticConcurrencyConflict, ProviderReferenceAlreadyObserved, RecordedEvent, SettlementAlreadyRequestedForWinningBid


EVENT_TYPES: dict[str, type[DomainEvent]] = {
    event.__name__: event for event in (
        AuctionOpened, CompetitiveBidPlaced, WinningBidAccepted, AuctionExpired,
        CompanyOnboarded, RiskAppetiteChanged,
        SettlementRequested, SettlementConfirmed, SettlementRejected, LateSettlementDetected, RefundRequested,
        RefundCompleted, RefundFailed,
    )
}

# Event names are the public contract.  Evolution is explicit and readers can
# upcast historic payloads before creating a current domain event.
EVENT_SCHEMA_VERSIONS: dict[str, int] = {name: 1 for name in EVENT_TYPES}
EVENT_SCHEMA_VERSIONS["SettlementRequested"] = 2


def upcast_event(event_name: str, schema_version: int, payload: dict[str, Any]) -> tuple[int, dict[str, Any]]:
    if event_name not in EVENT_TYPES:
        raise ValueError(f"unsupported event contract: {event_name}")
    current = EVENT_SCHEMA_VERSIONS[event_name]
    if schema_version > current:
        raise ValueError(f"{event_name} v{schema_version} is newer than this reader")
    if event_name == "SettlementRequested" and schema_version == 1:
        # v1 was emitted before process causality was stored in the fact.  It
        # remains replayable, with the absence recorded explicitly.
        return current, {**payload, "winning_bid_event_id": None}
    # Future migrations are registered here as pure vN -> vN+1 transforms.
    return current, payload


def _decode_value(name: str, value: Any) -> Any:
    if name in {"event_id"}:
        return UUID(value)
    if name in {"occurred_at", "expired_at"}:
        return datetime.fromisoformat(value)
    if name in {"reserve_price", "offer", "accepted_offer", "opening_capital", "risk_appetite", "former_appetite", "new_appetite", "amount"}:
        return Decimal(value)
    return value


class SqliteEventStore:
    """Hermetic adapter used by Bazel tests and the local deterministic TUI."""

    def __init__(self, database: str = ":memory:") -> None:
        if database != ":memory:":
            Path(database).parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(database)
        self.connection.row_factory = sqlite3.Row
        self._migrate()

    def _migrate(self) -> None:
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS event_stream (
              global_position INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE,
              stream_id TEXT NOT NULL, stream_version INTEGER NOT NULL,
              event_name TEXT NOT NULL, payload TEXT NOT NULL, occurred_at TEXT NOT NULL,
              schema_version INTEGER NOT NULL DEFAULT 1,
              correlation_id TEXT NOT NULL, causation_id TEXT, actor_id TEXT, tenant_id TEXT,
              UNIQUE(stream_id, stream_version)
            );
            CREATE TABLE IF NOT EXISTS command_receipt (
              idempotency_key TEXT PRIMARY KEY, stream_id TEXT NOT NULL, command_name TEXT NOT NULL,
              request_hash TEXT NOT NULL, actor_id TEXT NOT NULL, tenant_id TEXT NOT NULL, resulting_version INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS transactional_outbox (
              event_id TEXT PRIMARY KEY, topic TEXT NOT NULL, partition_key TEXT NOT NULL,
              body TEXT NOT NULL, published_at TEXT
            );
            CREATE TABLE IF NOT EXISTS auction_overview (
              auction_id TEXT PRIMARY KEY, resource TEXT NOT NULL, quantity INTEGER NOT NULL,
              reserve_price TEXT NOT NULL, leading_company_id TEXT, leading_offer TEXT,
              lifecycle TEXT NOT NULL, stream_version INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS projection_receipt (
              consumer_name TEXT NOT NULL, event_id TEXT NOT NULL, PRIMARY KEY(consumer_name, event_id)
            );
            CREATE TABLE IF NOT EXISTS provider_reference_claim (
              provider_reference TEXT PRIMARY KEY, settlement_stream_id TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS settlement_causation_claim (
              winning_bid_event_id TEXT PRIMARY KEY, settlement_stream_id TEXT NOT NULL
            );
            """
        )
        self.connection.commit()

    def read_stream(self, stream_id: str) -> list[RecordedEvent]:
        rows = self.connection.execute(
            "SELECT * FROM event_stream WHERE stream_id = ? ORDER BY stream_version", (stream_id,)
        ).fetchall()
        return [self._recorded(row) for row in rows]

    def idempotency_result(self, idempotency_key: str, *, stream_id: str, command_name: str, request_hash: str, actor_id: str | None, tenant_id: str | None) -> int | None:
        receipt = self.connection.execute(
            "SELECT * FROM command_receipt WHERE idempotency_key = ?", (idempotency_key,)
        ).fetchone()
        if receipt is None:
            return None
        identity = actor_id or ""
        tenant = tenant_id or ""
        if (receipt["stream_id"], receipt["command_name"], receipt["request_hash"], receipt["actor_id"], receipt["tenant_id"]) != (stream_id, command_name, request_hash, identity, tenant):
            raise IdempotencyKeyReused("idempotency key was already used for a different command")
        return int(receipt["resulting_version"])

    def all_events(self) -> list[RecordedEvent]:
        return [self._recorded(row) for row in self.connection.execute(
            "SELECT * FROM event_stream ORDER BY global_position"
        )]

    def events_after(self, global_position: int) -> list[RecordedEvent]:
        rows = self.connection.execute(
            "SELECT * FROM event_stream WHERE global_position > ? ORDER BY global_position", (global_position,)
        ).fetchall()
        return [self._recorded(row) for row in rows]

    def event_count(self) -> int:
        """Return telemetry without decoding the immutable event history."""
        row = self.connection.execute("SELECT COUNT(*) FROM event_stream").fetchone()
        return int(row[0])

    def append(self, *, stream_id: str, expected_version: int, events: list[DomainEvent], idempotency_key: str, command_name: str, request_hash: str, context: CommandContext) -> int:
        with self.connection:
            if not events:
                raise ValueError("an append requires at least one domain event")
            receipt = self.idempotency_result(idempotency_key, stream_id=stream_id, command_name=command_name, request_hash=request_hash, actor_id=context.actor_id, tenant_id=context.tenant_id)
            if receipt is not None:
                return receipt
            actual = self.connection.execute(
                "SELECT COALESCE(MAX(stream_version), 0) AS version FROM event_stream WHERE stream_id = ?",
                (stream_id,),
            ).fetchone()["version"]
            if actual != expected_version:
                raise OptimisticConcurrencyConflict(
                    f"{stream_id} advanced to version {actual}; command expected {expected_version}"
                )
            version = actual
            for event in events:
                if event.event_name == "SettlementRequested":
                    winning_bid_event_id = str(event.payload()["winning_bid_event_id"])
                    self.connection.execute(
                        "INSERT OR IGNORE INTO settlement_causation_claim VALUES (?, ?)",
                        (winning_bid_event_id, stream_id),
                    )
                    claim = self.connection.execute(
                        "SELECT settlement_stream_id FROM settlement_causation_claim WHERE winning_bid_event_id=?",
                        (winning_bid_event_id,),
                    ).fetchone()
                    if claim is None or claim["settlement_stream_id"] != stream_id:
                        raise SettlementAlreadyRequestedForWinningBid("winning bid already caused another settlement")
                if event.event_name in {"SettlementConfirmed", "LateSettlementDetected"}:
                    provider_reference = str(event.payload()["provider_reference"])
                    self.connection.execute(
                        "INSERT OR IGNORE INTO provider_reference_claim VALUES (?, ?)",
                        (provider_reference, stream_id),
                    )
                    claim = self.connection.execute(
                        "SELECT settlement_stream_id FROM provider_reference_claim WHERE provider_reference=?",
                        (provider_reference,),
                    ).fetchone()
                    if claim is None or claim["settlement_stream_id"] != stream_id:
                        raise ProviderReferenceAlreadyObserved("provider reference belongs to another settlement")
                version += 1
                body = json.dumps(event.payload(), sort_keys=True)
                self.connection.execute(
                    "INSERT INTO event_stream(event_id, stream_id, stream_version, event_name, payload, occurred_at, schema_version, correlation_id, causation_id, actor_id, tenant_id) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (str(event.event_id), stream_id, version, event.event_name, body, event.occurred_at.isoformat(),
                     EVENT_SCHEMA_VERSIONS[event.event_name], context.correlation_id, context.causation_id, context.actor_id, context.tenant_id),
                )
                position = self.connection.execute("SELECT global_position FROM event_stream WHERE event_id=?", (str(event.event_id),)).fetchone()[0]
                envelope = json.dumps({
                    "event_id": str(event.event_id), "event_name": event.event_name, "schema_version": EVENT_SCHEMA_VERSIONS[event.event_name],
                    "stream_id": stream_id, "stream_version": version, "global_position": position,
                    "occurred_at": event.occurred_at.isoformat(), "payload": event.payload(),
                    "correlation_id": context.correlation_id, "causation_id": context.causation_id,
                    "actor_id": context.actor_id, "tenant_id": context.tenant_id,
                }, sort_keys=True)
                self.connection.execute(
                    "INSERT INTO transactional_outbox VALUES (?, ?, ?, ?, NULL)",
                    (str(event.event_id), "thyphon.domain-events", stream_id, envelope),
                )
            self.connection.execute(
                "INSERT INTO command_receipt VALUES (?, ?, ?, ?, ?, ?, ?)",
                (idempotency_key, stream_id, command_name, request_hash, context.actor_id or "", context.tenant_id or "", version),
            )
            return version

    def unpublished_events(self) -> list[RecordedEvent]:
        rows = self.connection.execute(
            "SELECT e.* FROM event_stream e JOIN transactional_outbox o ON o.event_id = e.event_id "
            "WHERE o.published_at IS NULL ORDER BY e.occurred_at, e.event_id"
        ).fetchall()
        return [self._recorded(row) for row in rows]

    def unpublished_outbox(self) -> list[tuple[str, str, UUID, bytes]]:
        rows = self.connection.execute(
            "SELECT o.topic, o.partition_key, o.event_id, o.body FROM transactional_outbox o "
            "JOIN event_stream e ON e.event_id=o.event_id WHERE o.published_at IS NULL ORDER BY e.global_position"
        ).fetchall()
        return [(row["topic"], row["partition_key"], UUID(row["event_id"]), row["body"].encode()) for row in rows]

    def mark_published(self, event_id: UUID) -> None:
        self.connection.execute(
            "UPDATE transactional_outbox SET published_at = ? WHERE event_id = ?",
            (datetime.now(UTC).isoformat(), str(event_id)),
        )
        self.connection.commit()

    def _recorded(self, row: sqlite3.Row) -> RecordedEvent:
        event_type = EVENT_TYPES[row["event_name"]]
        raw = json.loads(row["payload"])
        schema_version, raw = upcast_event(row["event_name"], int(row["schema_version"]), raw)
        event = event_type(**{key: _decode_value(key, value) for key, value in raw.items()})
        return RecordedEvent(row["stream_id"], int(row["stream_version"]), event, int(row["global_position"]), schema_version)
