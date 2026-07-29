from dataclasses import dataclass


@dataclass(frozen=True)
class CompleteRefund:
    settlement_id: str
    provider_reference: str
