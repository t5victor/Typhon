from dataclasses import dataclass


@dataclass(frozen=True)
class RejectSettlement:
    settlement_id: str
    rejection_reason: str
    idempotency_key: str
