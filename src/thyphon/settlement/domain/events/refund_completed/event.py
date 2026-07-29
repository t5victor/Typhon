from dataclasses import dataclass

from thyphon.shared.domain import DomainEvent


@dataclass(frozen=True, kw_only=True)
class RefundCompleted(DomainEvent):
    provider_reference: str
