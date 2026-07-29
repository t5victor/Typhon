from __future__ import annotations

import hashlib
import hmac
import json
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, cast
from uuid import uuid4

from fastapi import Depends, FastAPI, Header, HTTPException, Response
from pydantic import BaseModel, Field

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
from thyphon.shared.domain import CommandContext, DomainViolation, IdempotencyKeyReused, OptimisticConcurrencyConflict, ProviderReferenceAlreadyObserved


class OpenAuctionRequest(BaseModel):
    auction_id: str = Field(pattern=r"^[a-z0-9-]+$")
    resource: str = Field(min_length=1, max_length=80)
    quantity: int = Field(gt=0, le=1_000_000)
    reserve_price: Decimal = Field(gt=0, max_digits=18, decimal_places=2)


class PlaceCompetitiveBidRequest(BaseModel):
    company_id: str = Field(min_length=1, max_length=100)
    offer: Decimal = Field(gt=0, max_digits=18, decimal_places=2)
    expected_version: int | None = Field(default=None, ge=0, le=2_000_000_000)


class ProviderConfirmationRequest(BaseModel):
    provider_reference: str = Field(min_length=1, max_length=120)


class SettlementRejectionRequest(BaseModel):
    rejection_reason: str = Field(min_length=1, max_length=240)


class RefundFailureRequest(BaseModel):
    provider_reference: str = Field(min_length=1, max_length=120)
    failure_reason: str = Field(min_length=1, max_length=240)


@dataclass(frozen=True)
class Principal:
    actor_id: str
    role: str


def role_required(*allowed_roles: str):
    def resolve(x_api_key: str | None = Header(default=None, alias="X-Thyphon-API-Key")) -> Principal:
        raw_keys = os.environ.get("THYPHON_API_KEYS")
        if raw_keys is None:
            raise HTTPException(status_code=503, detail="API identity configuration is unavailable")
        try:
            entries = json.loads(raw_keys)
            entry = None if x_api_key is None else entries.get(x_api_key)
        except json.JSONDecodeError as error:
            raise HTTPException(status_code=503, detail="API identity configuration is invalid") from error
        if entry is None:
            raise HTTPException(status_code=401, detail="invalid API key")
        principal = Principal(actor_id=entry["actor_id"], role=entry["role"])
        if principal.role not in allowed_roles:
            raise HTTPException(status_code=403, detail="actor is not allowed to perform this intention")
        return principal
    return resolve


def verify_provider_signature(settlement_id: str, intention: str, idempotency_key: str, payload: dict[str, object], signature: str) -> None:
    """Verify a canonical, intention-bound provider callback before it reaches the domain."""
    secret = os.environ.get("THYPHON_PROVIDER_WEBHOOK_SECRET")
    if not secret:
        raise HTTPException(status_code=503, detail="payment webhook verification is unavailable")
    material = json.dumps({
        "settlement_id": settlement_id, "intention": intention,
        "idempotency_key": idempotency_key, "payload": payload,
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
            store.connection.close()
            projector.connection.close()

    app = FastAPI(
        title="Thyphon", version="0.3.0", description="Intent-led commodity allocation exchange", lifespan=lifespan,
    )

    def execute(action):
        try:
            return action()
        except DomainViolation as error:
            raise HTTPException(status_code=422, detail=str(error)) from error
        except (OptimisticConcurrencyConflict, IdempotencyKeyReused, ProviderReferenceAlreadyObserved) as error:
            raise HTTPException(status_code=409, detail=str(error)) from error

    def context(idempotency_key: str, principal: Principal, correlation_id: str | None) -> CommandContext:
        return CommandContext(
            idempotency_key=idempotency_key, correlation_id=correlation_id or str(uuid4()), actor_id=principal.actor_id,
        )

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"service": "Thyphon", "status": "live"}

    @app.get("/health/ready")
    async def readiness() -> dict[str, str]:
        try:
            with store.connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
        except Exception as error:
            raise HTTPException(status_code=503, detail="event store is unavailable") from error
        try:
            from aiokafka import AIOKafkaProducer
            producer = AIOKafkaProducer(
                bootstrap_servers=os.environ.get("THYPHON_KAFKA_BOOTSTRAP", "localhost:29092"), request_timeout_ms=2_000,
            )
            await producer.start()
            await producer.stop()
        except Exception as error:
            raise HTTPException(status_code=503, detail="event broker is unavailable") from error
        return {"service": "Thyphon", "status": "ready"}

    @app.post("/commands/auctions/open", status_code=202)
    def open_auction(
        request: OpenAuctionRequest, idempotency_key: str = Header(alias="Idempotency-Key"),
        correlation_id: str | None = Header(default=None, alias="X-Correlation-ID"),
        principal: Principal = Depends(role_required("supplier")),
    ):
        version = execute(lambda: commands.open_auction(
            OpenAuction(request.auction_id, request.resource, request.quantity, request.reserve_price),
            context(idempotency_key, principal, correlation_id),
        ))
        return {"auction_id": request.auction_id, "status": "accepted", "expected_version": version}

    @app.post("/commands/auctions/{auction_id}/competitive-bids", status_code=202)
    def place_competitive_bid(
        auction_id: str, request: PlaceCompetitiveBidRequest, idempotency_key: str = Header(alias="Idempotency-Key"),
        correlation_id: str | None = Header(default=None, alias="X-Correlation-ID"),
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
        auction_id: str, idempotency_key: str = Header(alias="Idempotency-Key"),
        correlation_id: str | None = Header(default=None, alias="X-Correlation-ID"),
        principal: Principal = Depends(role_required("operator")),
    ):
        version = execute(lambda: commands.accept_winning_bid(
            AcceptWinningBid(auction_id), context(idempotency_key, principal, correlation_id),
        ))
        return {"auction_id": auction_id, "status": "accepted", "expected_version": version}

    def signed_settlement_action(intention: str, settlement_id: str, request: BaseModel, idempotency_key: str, signature: str) -> None:
        verify_provider_signature(settlement_id, intention, idempotency_key, request.model_dump(mode="json"), signature)

    @app.post("/commands/settlements/{settlement_id}/confirm", status_code=202)
    def confirm_settlement(
        settlement_id: str, request: ProviderConfirmationRequest, idempotency_key: str = Header(alias="Idempotency-Key"),
        correlation_id: str | None = Header(default=None, alias="X-Correlation-ID"),
        signature: str = Header(alias="X-Thyphon-Signature"), principal: Principal = Depends(role_required("payment-provider")),
    ):
        signed_settlement_action("confirm-settlement", settlement_id, request, idempotency_key, signature)
        version = execute(lambda: settlements.confirm_settlement(
            ConfirmSettlement(settlement_id, request.provider_reference), context(idempotency_key, principal, correlation_id),
        ))
        return {"settlement_id": settlement_id, "status": "accepted", "expected_version": version}

    @app.post("/commands/settlements/{settlement_id}/reject", status_code=202)
    def reject_settlement(
        settlement_id: str, request: SettlementRejectionRequest, idempotency_key: str = Header(alias="Idempotency-Key"),
        correlation_id: str | None = Header(default=None, alias="X-Correlation-ID"),
        signature: str = Header(alias="X-Thyphon-Signature"), principal: Principal = Depends(role_required("payment-provider")),
    ):
        signed_settlement_action("reject-settlement", settlement_id, request, idempotency_key, signature)
        version = execute(lambda: settlements.reject_settlement(
            RejectSettlement(settlement_id, request.rejection_reason), context(idempotency_key, principal, correlation_id),
        ))
        return {"settlement_id": settlement_id, "status": "accepted", "expected_version": version}

    @app.post("/commands/settlements/{settlement_id}/refund-completed", status_code=202)
    def complete_refund(
        settlement_id: str, request: ProviderConfirmationRequest, idempotency_key: str = Header(alias="Idempotency-Key"),
        correlation_id: str | None = Header(default=None, alias="X-Correlation-ID"),
        signature: str = Header(alias="X-Thyphon-Signature"), principal: Principal = Depends(role_required("payment-provider")),
    ):
        signed_settlement_action("refund-completed", settlement_id, request, idempotency_key, signature)
        version = execute(lambda: settlements.complete_refund(
            CompleteRefund(settlement_id, request.provider_reference), context(idempotency_key, principal, correlation_id),
        ))
        return {"settlement_id": settlement_id, "status": "accepted", "expected_version": version}

    @app.post("/commands/settlements/{settlement_id}/refund-failed", status_code=202)
    def fail_refund(
        settlement_id: str, request: RefundFailureRequest, idempotency_key: str = Header(alias="Idempotency-Key"),
        correlation_id: str | None = Header(default=None, alias="X-Correlation-ID"),
        signature: str = Header(alias="X-Thyphon-Signature"), principal: Principal = Depends(role_required("payment-provider")),
    ):
        signed_settlement_action("refund-failed", settlement_id, request, idempotency_key, signature)
        version = execute(lambda: settlements.fail_refund(
            FailRefund(settlement_id, request.provider_reference, request.failure_reason), context(idempotency_key, principal, correlation_id),
        ))
        return {"settlement_id": settlement_id, "status": "accepted", "expected_version": version}

    @app.get("/queries/auctions/{auction_id}")
    def auction_overview(auction_id: str, response: Response, minimum_version: int | None = None):
        overview = cast(dict[str, Any] | None, projector.overview(auction_id))
        if minimum_version is not None and (overview is None or overview["stream_version"] < minimum_version):
            response.status_code = 202
            response.headers["Retry-After"] = "1"
            return {"auction_id": auction_id, "status": "projection_pending", "minimum_version": minimum_version}
        if overview is None:
            raise HTTPException(status_code=404, detail="auction overview not projected yet")
        return dict(overview)

    return app
