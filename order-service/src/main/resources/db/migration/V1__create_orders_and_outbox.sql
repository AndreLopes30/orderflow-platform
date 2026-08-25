CREATE TABLE orders (
    id UUID PRIMARY KEY,
    customer_id VARCHAR(100) NOT NULL,
    total NUMERIC(19, 2) NOT NULL,
    status VARCHAR(20) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    version BIGINT NOT NULL DEFAULT 0,
    CONSTRAINT chk_orders_customer_id_not_blank CHECK (btrim(customer_id) <> ''),
    CONSTRAINT chk_orders_total_positive CHECK (total > 0),
    CONSTRAINT chk_orders_status CHECK (status IN ('CREATED', 'PROCESSING', 'COMPLETED', 'CANCELLED'))
);

CREATE INDEX idx_orders_customer_created_at ON orders (customer_id, created_at DESC);

CREATE TABLE outbox_events (
    id UUID PRIMARY KEY,
    aggregate_id UUID NOT NULL,
    aggregate_type VARCHAR(50) NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL,
    locked_at TIMESTAMPTZ,
    last_error VARCHAR(1000),
    CONSTRAINT chk_outbox_attempts_non_negative CHECK (attempts >= 0)
);

CREATE INDEX idx_outbox_pending
    ON outbox_events (next_attempt_at, occurred_at)
    WHERE published_at IS NULL;
