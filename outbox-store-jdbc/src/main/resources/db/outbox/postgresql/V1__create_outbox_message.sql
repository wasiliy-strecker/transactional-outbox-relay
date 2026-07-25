CREATE TABLE outbox_message (
    id UUID PRIMARY KEY,
    destination VARCHAR(249) NOT NULL,
    partition_key VARCHAR(512) NOT NULL,
    event_type VARCHAR(200) NOT NULL,
    schema_version VARCHAR(64) NOT NULL,
    aggregate_type VARCHAR(200) NOT NULL,
    aggregate_id VARCHAR(512) NOT NULL,
    aggregate_version BIGINT NOT NULL CHECK (aggregate_version > 0),
    occurred_at TIMESTAMPTZ NOT NULL,
    content_type VARCHAR(128) NOT NULL,
    headers JSONB NOT NULL,
    payload BYTEA NOT NULL CHECK (octet_length(payload) > 0),
    status VARCHAR(16) NOT NULL CHECK (status IN ('PENDING', 'IN_FLIGHT', 'PUBLISHED', 'DEAD')),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    available_at TIMESTAMPTZ NOT NULL,
    lease_owner VARCHAR(128),
    lease_token UUID,
    lease_until TIMESTAMPTZ,
    published_at TIMESTAMPTZ,
    broker_partition INTEGER,
    broker_offset BIGINT,
    last_error_type VARCHAR(200),
    last_error_message VARCHAR(1024),
    last_error_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT outbox_lease_shape CHECK (
        (status = 'IN_FLIGHT' AND lease_owner IS NOT NULL AND lease_token IS NOT NULL AND lease_until IS NOT NULL)
        OR
        (status <> 'IN_FLIGHT' AND lease_owner IS NULL AND lease_token IS NULL AND lease_until IS NULL)
    )
);

CREATE INDEX outbox_message_available_idx
    ON outbox_message (available_at, created_at, id)
    WHERE status = 'PENDING';

CREATE INDEX outbox_message_expired_lease_idx
    ON outbox_message (lease_until, id)
    WHERE status = 'IN_FLIGHT';

CREATE INDEX outbox_message_published_idx
    ON outbox_message (published_at, id)
    WHERE status = 'PUBLISHED';

CREATE INDEX outbox_message_aggregate_idx
    ON outbox_message (aggregate_type, aggregate_id, aggregate_version);
