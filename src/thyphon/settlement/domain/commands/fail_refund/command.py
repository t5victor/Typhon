from dataclasses import dataclass


@dataclass(frozen=True)
class FailRefund:
    settlement_id: str
    provider_reference: str
    failure_reason: str
    idempotency_key: str
