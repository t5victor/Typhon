CREATE TABLE event_stream (
    event_id UUID PRIMARY KEY,
    stream_id TEXT NOT NULL,
    stream_version BIGINT NOT NULL,
    event_name TEXT NOT NULL,
    payload JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    UNIQUE(stream_id, stream_version)
);

CREATE TABLE command_receipt (
    idempotency_key TEXT PRIMARY KEY,
    stream_id TEXT NOT NULL,
    command_name TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    resulting_version BIGINT NOT NULL,
    accepted_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE transactional_outbox (
    event_id UUID PRIMARY KEY REFERENCES event_stream(event_id),
    topic TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    body JSONB NOT NULL, -- event_name, event_id, stream_id, payload and occurred_at
    published_at TIMESTAMPTZ
);

CREATE TABLE auction_overview (
    auction_id TEXT PRIMARY KEY,
    resource TEXT NOT NULL,
    quantity BIGINT NOT NULL,
    reserve_price NUMERIC(18, 2) NOT NULL,
    leading_company_id TEXT,
    leading_offer NUMERIC(18, 2),
    lifecycle TEXT NOT NULL,
    stream_version BIGINT NOT NULL
);

CREATE TABLE projection_receipt (
    consumer_name TEXT NOT NULL,
    event_id UUID NOT NULL,
    PRIMARY KEY (consumer_name, event_id)
);

CREATE TABLE provider_reference_claim (
    provider_reference TEXT PRIMARY KEY,
    settlement_stream_id TEXT NOT NULL
);

CREATE TABLE process_checkpoint (
    process_name TEXT PRIMARY KEY,
    last_observed_at TIMESTAMPTZ NOT NULL
);
