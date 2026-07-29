from __future__ import annotations

import os
import time
from decimal import Decimal

from fastapi import FastAPI, Header, HTTPException, Response
from pydantic import BaseModel, Field

from thyphon.application.auction_commands import AuctionCommandHandler
from thyphon.auction.domain.commands.open_auction.command import OpenAuction
from thyphon.auction.domain.commands.place_competitive_bid.command import PlaceCompetitiveBid
from thyphon.infrastructure.postgres_event_store import PostgresEventStore
from thyphon.projections.postgres_auction_overview import PostgresAuctionOverviewProjector
from thyphon.application.settlement_commands import SettlementCommandHandler
from thyphon.settlement.domain.commands.confirm_settlement.command import ConfirmSettlement
from thyphon.settlement.domain.commands.reject_settlement.command import RejectSettlement
from thyphon.settlement.domain.commands.request_settlement.command import RequestSettlement
from thyphon.shared.domain import DomainViolation, OptimisticConcurrencyConflict


class OpenAuctionRequest(BaseModel):
    auction_id: str = Field(pattern=r"^[a-z0-9-]+$")
    resource: str = Field(min_length=1, max_length=80)
    quantity: int = Field(gt=0)
    reserve_price: Decimal = Field(gt=0)


class PlaceCompetitiveBidRequest(BaseModel):
    company_id: str = Field(min_length=1, max_length=100)
    offer: Decimal = Field(gt=0)
    expected_version: int | None = Field(default=None, ge=0)


class RequestSettlementRequest(BaseModel):
    settlement_id: str = Field(pattern=r"^[a-z0-9-]+$")
    auction_id: str = Field(pattern=r"^[a-z0-9-]+$")
    payer_company_id: str = Field(min_length=1, max_length=100)
    amount: Decimal = Field(gt=0)


class ProviderConfirmationRequest(BaseModel):
    provider_reference: str = Field(min_length=1, max_length=120)


class SettlementRejectionRequest(BaseModel):
    rejection_reason: str = Field(min_length=1, max_length=240)


def create_app():
    dsn = os.environ.get("THYPHON_DATABASE_URL", "postgresql://thyphon:thyphon@localhost:54329/thyphon")
    store = PostgresEventStore(dsn)
    commands = AuctionCommandHandler(store)
    settlements = SettlementCommandHandler(store)
    projector = PostgresAuctionOverviewProjector(dsn)
    app = FastAPI(title="Thyphon", version="0.2.0", description="Intent-led commodity allocation exchange")

    def execute(action):
        try:
            return action()
        except DomainViolation as error:
            raise HTTPException(status_code=422, detail=str(error)) from error
        except OptimisticConcurrencyConflict as error:
            raise HTTPException(status_code=409, detail=str(error)) from error

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"service": "Thyphon", "status": "ready"}

    @app.post("/commands/auctions/open", status_code=202)
    def open_auction(request: OpenAuctionRequest, idempotency_key: str = Header(alias="Idempotency-Key")):
        version = execute(lambda: commands.open_auction(OpenAuction(
            auction_id=request.auction_id, resource=request.resource, quantity=request.quantity,
            reserve_price=request.reserve_price, idempotency_key=idempotency_key,
        )))
        return {"auction_id": request.auction_id, "status": "accepted", "expected_version": version}

    @app.post("/commands/auctions/{auction_id}/competitive-bids", status_code=202)
    def place_competitive_bid(
        auction_id: str, request: PlaceCompetitiveBidRequest,
        idempotency_key: str = Header(alias="Idempotency-Key"),
    ):
        version = execute(lambda: commands.place_competitive_bid(PlaceCompetitiveBid(
            auction_id=auction_id, company_id=request.company_id, offer=request.offer,
            idempotency_key=idempotency_key, expected_version=request.expected_version,
        )))
        return {"auction_id": auction_id, "status": "accepted", "expected_version": version}

    @app.post("/commands/settlements/request", status_code=202)
    def request_settlement(request: RequestSettlementRequest, idempotency_key: str = Header(alias="Idempotency-Key")):
        version = execute(lambda: settlements.request_settlement(RequestSettlement(
            settlement_id=request.settlement_id, auction_id=request.auction_id,
            payer_company_id=request.payer_company_id, amount=request.amount,
            idempotency_key=idempotency_key,
        )))
        return {"settlement_id": request.settlement_id, "status": "accepted", "expected_version": version}

    @app.post("/commands/settlements/{settlement_id}/confirm", status_code=202)
    def confirm_settlement(settlement_id: str, request: ProviderConfirmationRequest, idempotency_key: str = Header(alias="Idempotency-Key")):
        version = execute(lambda: settlements.confirm_settlement(ConfirmSettlement(
            settlement_id=settlement_id, provider_reference=request.provider_reference, idempotency_key=idempotency_key,
        )))
        return {"settlement_id": settlement_id, "status": "accepted", "expected_version": version}

    @app.post("/commands/settlements/{settlement_id}/reject", status_code=202)
    def reject_settlement(settlement_id: str, request: SettlementRejectionRequest, idempotency_key: str = Header(alias="Idempotency-Key")):
        version = execute(lambda: settlements.reject_settlement(RejectSettlement(
            settlement_id=settlement_id, rejection_reason=request.rejection_reason, idempotency_key=idempotency_key,
        )))
        return {"settlement_id": settlement_id, "status": "accepted", "expected_version": version}

    @app.get("/queries/auctions/{auction_id}")
    def auction_overview(auction_id: str, minimum_version: int | None = None, response: Response = None):
        deadline = time.monotonic() + 1.0
        overview = projector.overview(auction_id)
        while minimum_version is not None and (overview is None or overview["stream_version"] < minimum_version):
            if time.monotonic() >= deadline:
                response.status_code = 202
                return {"auction_id": auction_id, "status": "projection_pending", "minimum_version": minimum_version}
            time.sleep(0.025)
            overview = projector.overview(auction_id)
        if overview is None:
            raise HTTPException(status_code=404, detail="auction overview not projected yet")
        return dict(overview)

    @app.post("/operations/projections/rebuild-auction-overview", status_code=202)
    def rebuild_auction_overview():
        return {"events_replayed": projector.rebuild()}

    return app
