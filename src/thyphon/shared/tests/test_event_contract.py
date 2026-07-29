import json
import unittest
from decimal import Decimal
from uuid import uuid4

from thyphon.application.auction_commands import AuctionCommandHandler
from thyphon.auction.domain.commands.open_auction.command import OpenAuction
from thyphon.infrastructure.kafka_outbox_dispatcher import KafkaOutboxDispatcher
from thyphon.infrastructure.sqlite_event_store import EVENT_SCHEMA_VERSIONS, SqliteEventStore, upcast_event
from thyphon.projections.postgres_auction_overview import PostgresAuctionOverviewProjector
from thyphon.shared.domain import CommandContext, command_metadata


class EventContract(unittest.TestCase):
    def test_outbox_uses_a_versioned_traceable_event_envelope(self) -> None:
        store = SqliteEventStore()
        AuctionCommandHandler(store).open_auction(
            OpenAuction("gold-1", "Gold", 1, Decimal("100")),
            CommandContext("open-gold-1", "correlation-1", actor_id="nordic-mining", tenant_id="local-lab"),
        )
        published: list[bytes] = []
        KafkaOutboxDispatcher(store, lambda _topic, _key, body: published.append(body)).deliver_pending()
        envelope = json.loads(published[0])
        self.assertEqual(envelope["event_name"], "AuctionOpened")
        self.assertEqual(envelope["schema_version"], EVENT_SCHEMA_VERSIONS["AuctionOpened"])
        self.assertEqual(envelope["correlation_id"], "correlation-1")
        self.assertEqual(envelope["actor_id"], "nordic-mining")
        self.assertGreater(envelope["global_position"], 0)

    def test_domain_intention_has_no_transport_fields(self) -> None:
        fields = OpenAuction.__dataclass_fields__
        self.assertNotIn("idempotency_key", fields)
        self.assertNotIn("correlation_id", fields)

    def test_event_schema_version_must_be_positive(self) -> None:
        with self.assertRaisesRegex(ValueError, "not a valid"):
            upcast_event("AuctionOpened", 0, {})

    def test_decoder_rejects_an_envelope_whose_outer_id_differs_from_payload(self) -> None:
        store = SqliteEventStore()
        AuctionCommandHandler(store).open_auction(
            OpenAuction("gold-2", "Gold", 1, Decimal("100")),
            CommandContext("open-gold-2", "correlation-2", actor_id="nordic-mining", tenant_id="local-lab"),
        )
        published: list[bytes] = []
        KafkaOutboxDispatcher(store, lambda _topic, _key, body: published.append(body)).deliver_pending()
        envelope = json.loads(published[0])
        envelope["event_id"] = str(uuid4())
        with self.assertRaisesRegex(ValueError, "event_id"):
            PostgresAuctionOverviewProjector.decode(envelope)

    def test_command_hash_is_stable_for_nested_uuid_decimal_and_datetime_values(self) -> None:
        from dataclasses import dataclass
        from datetime import UTC, datetime
        from uuid import UUID

        @dataclass(frozen=True)
        class FingerprintProbe:
            causal_id: UUID
            values: dict[str, object]

        command = FingerprintProbe(
            UUID("00000000-0000-4000-8000-000000000001"),
            {"amount": Decimal("12.30"), "when": datetime(2026, 1, 1, tzinfo=UTC), "nested": [UUID("00000000-0000-4000-8000-000000000002")]},
        )
        self.assertEqual(command_metadata(command), command_metadata(command))


if __name__ == "__main__":
    unittest.main()
