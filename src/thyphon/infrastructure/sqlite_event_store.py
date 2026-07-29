from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
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
from thyphon.settlement.domain.events.settlement_confirmed.event import SettlementConfirmed
from thyphon.settlement.domain.events.settlement_rejected.event import SettlementRejected
from thyphon.settlement.domain.events.settlement_requested.event import SettlementRequested
from thyphon.shared.domain import DomainEvent, OptimisticConcurrencyConflict, RecordedEvent


EVENT_TYPES: dict[str, type[DomainEvent]] = {
    event.__name__: event for event in (
        AuctionOpened, CompetitiveBidPlaced, WinningBidAccepted, AuctionExpired,
        CompanyOnboarded, RiskAppetiteChanged,
        SettlementRequested, SettlementConfirmed, SettlementRejected, LateSettlementDetected, RefundRequested,
    )
}


def _decode_value(name: str, value: Any) -> Any:
    if name in {"event_id"}:
        return UUID(value)
    if name in {"occurred_at", "expired_at"}:
        return datetime.fromisoformat(value)
    if name in {"reserve_price", "offer", "accepted_offer", "opening_capital", "risk_appetite", "former_appetite", "new_appetite"}:
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
              event_id TEXT PRIMARY KEY, stream_id TEXT NOT NULL, stream_version INTEGER NOT NULL,
              event_name TEXT NOT NULL, payload TEXT NOT NULL, occurred_at TEXT NOT NULL,
              UNIQUE(stream_id, stream_version)
            );
            CREATE TABLE IF NOT EXISTS command_receipt (
              idempotency_key TEXT PRIMARY KEY, stream_id TEXT NOT NULL, resulting_version INTEGER NOT NULL
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
            """
        )
        self.connection.commit()

    def read_stream(self, stream_id: str) -> list[RecordedEvent]:
        rows = self.connection.execute(
            "SELECT * FROM event_stream WHERE stream_id = ? ORDER BY stream_version", (stream_id,)
        ).fetchall()
        return [self._recorded(row) for row in rows]

    def idempotency_result(self, idempotency_key: str) -> int | None:
        receipt = self.connection.execute(
            "SELECT resulting_version FROM command_receipt WHERE idempotency_key = ?", (idempotency_key,)
        ).fetchone()
        return None if receipt is None else int(receipt["resulting_version"])

    def all_events(self) -> list[RecordedEvent]:
        return [self._recorded(row) for row in self.connection.execute(
            "SELECT * FROM event_stream ORDER BY occurred_at, event_id"
        )]

    def append(self, *, stream_id: str, expected_version: int, events: list[DomainEvent], idempotency_key: str) -> int:
        with self.connection:
            receipt = self.idempotency_result(idempotency_key)
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
                version += 1
                body = json.dumps(event.payload(), sort_keys=True)
                self.connection.execute(
                    "INSERT INTO event_stream VALUES (?, ?, ?, ?, ?, ?)",
                    (str(event.event_id), stream_id, version, event.event_name, body, event.occurred_at.isoformat()),
                )
                self.connection.execute(
                    "INSERT INTO transactional_outbox VALUES (?, ?, ?, ?, NULL)",
                    (str(event.event_id), "thyphon.domain-events", stream_id, body),
                )
            self.connection.execute(
                "INSERT INTO command_receipt VALUES (?, ?, ?)", (idempotency_key, stream_id, version)
            )
            return version

    def unpublished_events(self) -> list[RecordedEvent]:
        rows = self.connection.execute(
            "SELECT e.* FROM event_stream e JOIN transactional_outbox o ON o.event_id = e.event_id "
            "WHERE o.published_at IS NULL ORDER BY e.occurred_at, e.event_id"
        ).fetchall()
        return [self._recorded(row) for row in rows]

    def mark_published(self, event_id: UUID) -> None:
        self.connection.execute(
            "UPDATE transactional_outbox SET published_at = ? WHERE event_id = ?",
            (datetime.now(UTC).isoformat(), str(event_id)),
        )
        self.connection.commit()

    def _recorded(self, row: sqlite3.Row) -> RecordedEvent:
        event_type = EVENT_TYPES[row["event_name"]]
        raw = json.loads(row["payload"])
        event = event_type(**{key: _decode_value(key, value) for key, value in raw.items()})
        return RecordedEvent(row["stream_id"], int(row["stream_version"]), event)
