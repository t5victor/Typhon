import json
import unittest
from decimal import Decimal

from thyphon.application.auction_commands import AuctionCommandHandler
from thyphon.auction.domain.commands.open_auction.command import OpenAuction
from thyphon.infrastructure.kafka_outbox_dispatcher import KafkaOutboxDispatcher
from thyphon.infrastructure.sqlite_event_store import EVENT_SCHEMA_VERSIONS, SqliteEventStore
from thyphon.shared.domain import CommandContext


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


if __name__ == "__main__":
    unittest.main()
