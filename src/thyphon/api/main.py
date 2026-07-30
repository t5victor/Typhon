from __future__ import annotations

import hashlib
import hmac
import json
import os
from time import monotonic
from time import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from decimal import Decimal
from collections.abc import Callable
from typing import Any, cast
from uuid import uuid4

from fastapi import Depends, FastAPI, Header, HTTPException, Path, Response
from pydantic import BaseModel, ConfigDict, Field

from thyphon.application.auction_commands import AuctionCommandHandler
from thyphon.application.settlement_commands import SettlementCommandHandler
from thyphon.auction.domain.commands.accept_winning_bid.command import AcceptWinningBid
from thyphon.auction.domain.commands.open_auction.command import OpenAuction
from thyphon.auction.domain.commands.place_competitive_bid.command import PlaceCompetitiveBid
from thyphon.infrastructure.postgres_event_store import PostgresEventStore
from thyphon.projections.postgres_auction_overview import PostgresAuctionOverviewProjector
from thyphon.settlement.domain.commands.complete_refund.command import CompleteRefund
from thyphon.settlement.domain.commands.confirm_settlement.command import ConfirmSettlement
from thyphon.settlement.domain.commands.fail_refund.command import FailRefund
from thyphon.settlement.domain.commands.reject_settlement.command import RejectSettlement
from thyphon.shared.domain import CommandContext, DomainViolation, IdempotencyKeyReused, InvalidSettlementCausation, OptimisticConcurrencyConflict, ProviderReferenceAlreadyObserved, SettlementAlreadyRequestedForWinningBid


class CommandRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")


class OpenAuctionRequest(CommandRequest):
    auction_id: str = Field(pattern=r"^[a-z0-9-]+$", max_length=80)
    resource: str = Field(min_length=1, max_length=80)
    quantity: int = Field(gt=0, le=1_000_000)
    reserve_price: Decimal = Field(gt=0, max_digits=18, decimal_places=2)


class PlaceCompetitiveBidRequest(CommandRequest):
    company_id: str = Field(min_length=1, max_length=100)
    offer: Decimal = Field(gt=0, max_digits=18, decimal_places=2)
    expected_version: int | None = Field(default=None, ge=0, le=2_000_000_000)


class ProviderConfirmationRequest(CommandRequest):
    provider_reference: str = Field(min_length=1, max_length=120)


class SettlementRejectionRequest(CommandRequest):
    rejection_reason: str = Field(min_length=1, max_length=240)


class RefundFailureRequest(CommandRequest):
    provider_reference: str = Field(min_length=1, max_length=120)
    failure_reason: str = Field(min_length=1, max_length=240)


@dataclass(frozen=True)
class Principal:
    actor_id: str
    role: str
    tenant_id: str


def role_required(*allowed_roles: str):
    def resolve(x_api_key: str | None = Header(default=None, alias="X-Thyphon-API-Key")) -> Principal:
        raw_keys = os.environ.get("THYPHON_API_KEYS")
        if raw_keys is None:
            raise HTTPException(status_code=503, detail="API identity configuration is unavailable")
        try:
            entries = json.loads(raw_keys)
            if not isinstance(entries, dict):
                raise TypeError("API keys must be a JSON object")
            entry = None if x_api_key is None else entries.get(x_api_key)
        except (json.JSONDecodeError, AttributeError, TypeError) as error:
            raise HTTPException(status_code=503, detail="API identity configuration is invalid") from error
        if entry is None:
            raise HTTPException(status_code=401, detail="invalid API key")
        try:
            principal = Principal(
                actor_id=entry["actor_id"], role=entry["role"], tenant_id=entry.get("tenant_id", "default"),
            )
        except (AttributeError, KeyError, TypeError) as error:
            raise HTTPException(status_code=503, detail="API identity configuration is invalid") from error
        if principal.role not in allowed_roles:
            raise HTTPException(status_code=403, detail="actor is not allowed to perform this intention")
        return principal
    return resolve


def verify_provider_signature(settlement_id: str, intention: str, idempotency_key: str, payload: dict[str, object], signature: str, timestamp: str) -> None:
    """Verify a canonical, intention-bound provider callback before it reaches the domain."""
    secret = os.environ.get("THYPHON_PROVIDER_WEBHOOK_SECRET")
    if not secret:
        raise HTTPException(status_code=503, detail="payment webhook verification is unavailable")
    try:
        issued_at = int(timestamp)
    except ValueError as error:
        raise HTTPException(status_code=401, detail="invalid payment-provider timestamp") from error
    if abs(int(time()) - issued_at) > 300:
        raise HTTPException(status_code=401, detail="expired payment-provider callback")
    material = json.dumps({
        "settlement_id": settlement_id, "intention": intention,
        "idempotency_key": idempotency_key, "timestamp": issued_at, "payload": payload,
    }, sort_keys=True, separators=(",", ":")).encode()
    expected = hmac.new(secret.encode(), material, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(signature, expected):
        raise HTTPException(status_code=401, detail="invalid payment-provider signature")


def create_app() -> FastAPI:
    dsn = os.environ.get("THYPHON_DATABASE_URL", "postgresql://thyphon:thyphon@localhost:54329/thyphon")
    store = PostgresEventStore(dsn)
    commands = AuctionCommandHandler(store)
    settlements = SettlementCommandHandler(store)
    projector = PostgresAuctionOverviewProjector(dsn)
    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        try:
            yield
        finally:
            store.close()
            projector.close()

    app = FastAPI(
        title="Thyphon", version="0.3.0", description="Intent-led commodity allocation exchange", lifespan=lifespan,
    )
    broker_ready_until = 0.0

    def execute(action: Callable[[], int]) -> int:
        try:
            return action()
        except (DomainViolation, InvalidSettlementCausation, ValueError) as error:
            raise HTTPException(status_code=422, detail=str(error)) from error
        except (OptimisticConcurrencyConflict, IdempotencyKeyReused, ProviderReferenceAlreadyObserved, SettlementAlreadyRequestedForWinningBid) as error:
            raise HTTPException(status_code=409, detail=str(error)) from error

    def context(idempotency_key: str, principal: Principal, correlation_id: str | None) -> CommandContext:
        return CommandContext(
            idempotency_key=idempotency_key, correlation_id=correlation_id or str(uuid4()), actor_id=principal.actor_id,
            tenant_id=principal.tenant_id,
        )

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"service": "Thyphon", "status": "live"}

    @app.get("/health/ready")
    async def readiness() -> dict[str, str]:
        nonlocal broker_ready_until
        try:
            with store.connection() as connection, connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
        except Exception as error:
            raise HTTPException(status_code=503, detail="event store is unavailable") from error
        if monotonic() >= broker_ready_until:
            try:
                from aiokafka import AIOKafkaProducer
                producer = AIOKafkaProducer(
                    bootstrap_servers=os.environ.get("THYPHON_KAFKA_BOOTSTRAP", "kafka:9092"), request_timeout_ms=2_000,
                )
                await producer.start()
                await producer.stop()
                broker_ready_until = monotonic() + 5.0
            except Exception as error:
                raise HTTPException(status_code=503, detail="event broker is unavailable") from error
        return {"service": "Thyphon", "status": "ready"}

    @app.post("/commands/auctions/open", status_code=202)
    def open_auction(
        request: OpenAuctionRequest, idempotency_key: str = Header(alias="Idempotency-Key", min_length=1, max_length=200),
        correlation_id: str | None = Header(default=None, alias="X-Correlation-ID", max_length=120),
        principal: Principal = Depends(role_required("supplier")),
    ):
        version = execute(lambda: commands.open_auction(
            OpenAuction(request.auction_id, request.resource, request.quantity, request.reserve_price),
            context(idempotency_key, principal, correlation_id),
        ))
        return {"auction_id": request.auction_id, "status": "accepted", "expected_version": version}

    @app.post("/commands/auctions/{auction_id}/competitive-bids", status_code=202)
    def place_competitive_bid(
        auction_id: str = Path(pattern=r"^[a-z0-9-]+$", max_length=80), request: PlaceCompetitiveBidRequest = ..., idempotency_key: str = Header(alias="Idempotency-Key", min_length=1, max_length=200),
        correlation_id: str | None = Header(default=None, alias="X-Correlation-ID", max_length=120),
        principal: Principal = Depends(role_required("bidder")),
    ):
        if request.company_id != principal.actor_id:
            raise HTTPException(status_code=403, detail="a bidder can only place offers for its own company")
        version = execute(lambda: commands.place_competitive_bid(
            PlaceCompetitiveBid(auction_id, request.company_id, request.offer, request.expected_version),
            context(idempotency_key, principal, correlation_id),
        ))
        return {"auction_id": auction_id, "status": "accepted", "expected_version": version}

    @app.post("/commands/auctions/{auction_id}/accept-winning-bid", status_code=202)
    def accept_winning_bid(
        auction_id: str = Path(pattern=r"^[a-z0-9-]+$", max_length=80), idempotency_key: str = Header(alias="Idempotency-Key", min_length=1, max_length=200),
        correlation_id: str | None = Header(default=None, alias="X-Correlation-ID", max_length=120),
        principal: Principal = Depends(role_required("operator")),
    ):
        version = execute(lambda: commands.accept_winning_bid(
            AcceptWinningBid(auction_id), context(idempotency_key, principal, correlation_id),
        ))
        return {"auction_id": auction_id, "status": "accepted", "expected_version": version}

    def signed_settlement_action(intention: str, settlement_id: str, request: BaseModel, idempotency_key: str, signature: str, timestamp: str) -> None:
        verify_provider_signature(settlement_id, intention, idempotency_key, request.model_dump(mode="json"), signature, timestamp)

    @app.post("/commands/settlements/{settlement_id}/confirm", status_code=202)
    def confirm_settlement(
        settlement_id: str = Path(pattern=r"^[a-z0-9-]+$", max_length=120), request: ProviderConfirmationRequest = ..., idempotency_key: str = Header(alias="Idempotency-Key", min_length=1, max_length=200),
        correlation_id: str | None = Header(default=None, alias="X-Correlation-ID", max_length=120),
        signature: str = Header(alias="X-Thyphon-Signature", min_length=64, max_length=64), timestamp: str = Header(alias="X-Thyphon-Timestamp", min_length=10, max_length=10), principal: Principal = Depends(role_required("payment-provider")),
    ):
        signed_settlement_action("confirm-settlement", settlement_id, request, idempotency_key, signature, timestamp)
        version = execute(lambda: settlements.confirm_settlement(
            ConfirmSettlement(settlement_id, request.provider_reference), context(idempotency_key, principal, correlation_id),
        ))
        return {"settlement_id": settlement_id, "status": "accepted", "expected_version": version}

    @app.post("/commands/settlements/{settlement_id}/reject", status_code=202)
    def reject_settlement(
        settlement_id: str = Path(pattern=r"^[a-z0-9-]+$", max_length=120), request: SettlementRejectionRequest = ..., idempotency_key: str = Header(alias="Idempotency-Key", min_length=1, max_length=200),
        correlation_id: str | None = Header(default=None, alias="X-Correlation-ID", max_length=120),
        signature: str = Header(alias="X-Thyphon-Signature", min_length=64, max_length=64), timestamp: str = Header(alias="X-Thyphon-Timestamp", min_length=10, max_length=10), principal: Principal = Depends(role_required("payment-provider")),
    ):
        signed_settlement_action("reject-settlement", settlement_id, request, idempotency_key, signature, timestamp)
        version = execute(lambda: settlements.reject_settlement(
            RejectSettlement(settlement_id, request.rejection_reason), context(idempotency_key, principal, correlation_id),
        ))
        return {"settlement_id": settlement_id, "status": "accepted", "expected_version": version}

    @app.post("/commands/settlements/{settlement_id}/refund-completed", status_code=202)
    def complete_refund(
        settlement_id: str = Path(pattern=r"^[a-z0-9-]+$", max_length=120), request: ProviderConfirmationRequest = ..., idempotency_key: str = Header(alias="Idempotency-Key", min_length=1, max_length=200),
        correlation_id: str | None = Header(default=None, alias="X-Correlation-ID", max_length=120),
        signature: str = Header(alias="X-Thyphon-Signature", min_length=64, max_length=64), timestamp: str = Header(alias="X-Thyphon-Timestamp", min_length=10, max_length=10), principal: Principal = Depends(role_required("payment-provider")),
    ):
        signed_settlement_action("refund-completed", settlement_id, request, idempotency_key, signature, timestamp)
        version = execute(lambda: settlements.complete_refund(
            CompleteRefund(settlement_id, request.provider_reference), context(idempotency_key, principal, correlation_id),
        ))
        return {"settlement_id": settlement_id, "status": "accepted", "expected_version": version}

    @app.post("/commands/settlements/{settlement_id}/refund-failed", status_code=202)
    def fail_refund(
        settlement_id: str = Path(pattern=r"^[a-z0-9-]+$", max_length=120), request: RefundFailureRequest = ..., idempotency_key: str = Header(alias="Idempotency-Key", min_length=1, max_length=200),
        correlation_id: str | None = Header(default=None, alias="X-Correlation-ID", max_length=120),
        signature: str = Header(alias="X-Thyphon-Signature", min_length=64, max_length=64), timestamp: str = Header(alias="X-Thyphon-Timestamp", min_length=10, max_length=10), principal: Principal = Depends(role_required("payment-provider")),
    ):
        signed_settlement_action("refund-failed", settlement_id, request, idempotency_key, signature, timestamp)
        version = execute(lambda: settlements.fail_refund(
            FailRefund(settlement_id, request.provider_reference, request.failure_reason), context(idempotency_key, principal, correlation_id),
        ))
        return {"settlement_id": settlement_id, "status": "accepted", "expected_version": version}

    @app.get("/queries/auctions/{auction_id}")
    def auction_overview(response: Response, auction_id: str = Path(pattern=r"^[a-z0-9-]+$", max_length=80), minimum_version: int | None = None):
        overview = cast(dict[str, Any] | None, projector.overview(auction_id))
        if minimum_version is not None and (overview is None or overview["stream_version"] < minimum_version):
            response.status_code = 202
            response.headers["Retry-After"] = "1"
            return {"auction_id": auction_id, "status": "projection_pending", "minimum_version": minimum_version}
        if overview is None:
            raise HTTPException(status_code=404, detail="auction overview not projected yet")
        return dict(overview)

    return app
