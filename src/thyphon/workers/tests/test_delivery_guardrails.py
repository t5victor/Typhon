from __future__ import annotations

import unittest
from contextlib import AbstractContextManager, nullcontext
from uuid import uuid4

from thyphon.workers.main import (
    _event_id_or_none,
    _is_infrastructure_error,
    _locked_active_redrive_attempt,
    _dead_letter_preview,
    _parse_envelope,
    _redrive_attempt_id,
    _redrive_target,
    _redrive_delivery_requires_rebuild,
)


class _Cursor:
    def __init__(self, row: tuple[object, object, object] | None) -> None:
        self.row = row
        self.statements: list[str] = []

    def __enter__(self) -> _Cursor:
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def execute(self, statement: str, _: object) -> None:
        self.statements.append(statement)

    def fetchone(self) -> tuple[object, object, object] | None:
        return self.row


class _FailureStore:
    def __init__(self, row: tuple[object, object, object] | None) -> None:
        self.cursor_instance = _Cursor(row)

    def transaction(self) -> AbstractContextManager[None]:
        return nullcontext()

    def cursor(self) -> _Cursor:
        return self.cursor_instance


class DeliveryGuardrails(unittest.TestCase):
    def test_tombstone_invalid_json_and_non_object_are_rejected_before_processing(self) -> None:
        for raw in (None, b"not-json", b"[]"):
            with self.assertRaises(ValueError):
                _parse_envelope(raw)

    def test_missing_or_invalid_event_id_is_classified_as_raw_failure(self) -> None:
        self.assertIsNone(_event_id_or_none({}))
        self.assertIsNone(_event_id_or_none({"event_id": "not-a-uuid"}))

    def test_invalid_redrive_header_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            _redrive_attempt_id([("thyphon-redrive-attempt", b"invalid")])

    def test_redrive_target_is_decoded_independently_from_the_attempt_id(self) -> None:
        self.assertEqual(
            "auction-overview-v1",
            _redrive_target([("thyphon-redrive-consumer", b"auction-overview-v1")]),
        )
        with self.assertRaises(ValueError):
            _redrive_target([("thyphon-redrive-consumer", b"")])

    def test_transient_infrastructure_errors_are_not_contract_poison(self) -> None:
        self.assertTrue(_is_infrastructure_error(TimeoutError("pool temporarily exhausted")))
        self.assertTrue(_is_infrastructure_error(ConnectionError("database reconnecting")))
        self.assertFalse(_is_infrastructure_error(ValueError("invalid envelope")))

    def test_active_redrive_rebuilds_even_before_dispatcher_records_published_at(self) -> None:
        attempt_id = uuid4()
        self.assertTrue(_redrive_delivery_requires_rebuild(
            attempt_status="pending", failure_resolved_at=None, active_attempt_id=attempt_id,
            attempt_id=attempt_id,
        ))

    def test_duplicate_resolved_redrive_is_a_successful_noop(self) -> None:
        attempt_id = uuid4()
        self.assertFalse(_redrive_delivery_requires_rebuild(
            attempt_status="resolved", failure_resolved_at="2026-07-29T20:00:00Z",
            active_attempt_id=attempt_id, attempt_id=attempt_id,
        ))

    def test_unresolved_attempt_cannot_rebuild_a_resolved_failure(self) -> None:
        attempt_id = uuid4()
        with self.assertRaisesRegex(ValueError, "failure is already resolved"):
            _redrive_delivery_requires_rebuild(
                attempt_status="published", failure_resolved_at="2026-07-29T20:00:00Z",
                active_attempt_id=attempt_id, attempt_id=attempt_id,
            )

    def test_delivery_in_dispatch_persistence_window_is_an_active_redrive(self) -> None:
        attempt_id = uuid4()
        store = _FailureStore(("pending", None, attempt_id))
        with _locked_active_redrive_attempt(
            store, attempt_id=attempt_id, consumer_name="auction-overview-v1", event_id=uuid4(),
        ) as requires_rebuild:
            self.assertTrue(requires_rebuild)
        self.assertNotIn("published_at", store.cursor_instance.statements[0])
        self.assertEqual(3, len(store.cursor_instance.statements))

    def test_resolved_redrive_delivery_does_not_write_or_rebuild_again(self) -> None:
        attempt_id = uuid4()
        store = _FailureStore(("resolved", "2026-07-29T20:00:00Z", attempt_id))
        with _locked_active_redrive_attempt(
            store, attempt_id=attempt_id, consumer_name="auction-overview-v1", event_id=uuid4(),
        ) as requires_rebuild:
            self.assertFalse(requires_rebuild)
        self.assertEqual(1, len(store.cursor_instance.statements))

    def test_unknown_or_mismatched_redrive_attempt_remains_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "unknown or belongs to another event"):
            with _locked_active_redrive_attempt(
                _FailureStore(None), attempt_id=uuid4(), consumer_name="auction-overview-v1", event_id=uuid4(),
            ):
                pass

    def test_failed_redrive_duplicate_is_also_a_noop(self) -> None:
        self.assertFalse(_redrive_delivery_requires_rebuild(
            attempt_status="failed", failure_resolved_at=None, active_attempt_id=None, attempt_id=uuid4(),
        ))

    def test_dead_letter_preview_is_bounded_and_never_repeats_the_full_message(self) -> None:
        raw = b"x" * 800_000
        preview = _dead_letter_preview(raw)
        assert preview is not None
        self.assertLess(len(preview), 5_000)


if __name__ == "__main__":
    unittest.main()
