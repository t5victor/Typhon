CREATE TABLE event_stream (
    event_id UUID PRIMARY KEY,
    global_position BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    stream_id TEXT NOT NULL,
    stream_version BIGINT NOT NULL,
    event_name TEXT NOT NULL,
    payload JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    schema_version SMALLINT NOT NULL DEFAULT 1,
    correlation_id TEXT NOT NULL,
    causation_id TEXT,
    actor_id TEXT,
    tenant_id TEXT,
    UNIQUE(stream_id, stream_version)
);

CREATE TABLE event_stream_head (
    stream_id TEXT PRIMARY KEY,
    current_version BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE command_receipt (
    idempotency_key TEXT PRIMARY KEY,
    stream_id TEXT NOT NULL,
    command_name TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
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

CREATE TABLE settlement_causation_claim (
    winning_bid_event_id UUID PRIMARY KEY REFERENCES event_stream(event_id),
    settlement_stream_id TEXT NOT NULL
);

CREATE TABLE process_checkpoint (
    process_name TEXT PRIMARY KEY,
    last_observed_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE projection_failure (
    consumer_name TEXT NOT NULL,
    event_id UUID NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NOT NULL,
    quarantined_at TIMESTAMPTZ,
    redriven_at TIMESTAMPTZ,
    resolved_at TIMESTAMPTZ,
    redrive_count INTEGER NOT NULL DEFAULT 0,
    active_redrive_attempt_id UUID,
    PRIMARY KEY (consumer_name, event_id)
);

CREATE TABLE projection_raw_failure (
    consumer_name TEXT NOT NULL,
    topic TEXT NOT NULL,
    partition_id INTEGER NOT NULL,
    message_offset BIGINT NOT NULL,
    raw_value BYTEA,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NOT NULL,
    quarantined_at TIMESTAMPTZ,
    PRIMARY KEY (consumer_name, topic, partition_id, message_offset)
);

CREATE TABLE projection_redrive_attempt (
    attempt_id UUID PRIMARY KEY,
    consumer_name TEXT NOT NULL,
    event_id UUID NOT NULL,
    envelope JSONB NOT NULL,
    requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ,
    resolved_at TIMESTAMPTZ
);
