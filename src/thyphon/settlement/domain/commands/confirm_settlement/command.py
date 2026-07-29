from dataclasses import dataclass


@dataclass(frozen=True)
class ConfirmSettlement:
    settlement_id: str
    provider_reference: str
    idempotency_key: str
