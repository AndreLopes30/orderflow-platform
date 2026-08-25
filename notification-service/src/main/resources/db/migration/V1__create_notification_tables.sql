CREATE TABLE processed_events (
    event_id UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE notification_records (
    id UUID PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    order_id UUID NOT NULL,
    customer_id VARCHAR(100) NOT NULL,
    channel VARCHAR(30) NOT NULL,
    status VARCHAR(20) NOT NULL,
    sent_at TIMESTAMPTZ NOT NULL,
    message VARCHAR(255) NOT NULL,
    CONSTRAINT fk_notification_processed_event
        FOREIGN KEY (event_id) REFERENCES processed_events (event_id),
    CONSTRAINT chk_notification_customer_not_blank CHECK (btrim(customer_id) <> ''),
    CONSTRAINT chk_notification_status CHECK (status = 'SENT')
);

CREATE INDEX idx_notification_order_id ON notification_records (order_id);

CREATE TABLE dead_letter_events (
    id UUID PRIMARY KEY,
    deduplication_key VARCHAR(64) NOT NULL UNIQUE,
    event_id UUID,
    order_id UUID,
    payload TEXT NOT NULL,
    failed_at TIMESTAMPTZ NOT NULL,
    reason VARCHAR(500) NOT NULL
);

CREATE INDEX idx_dead_letter_event_id ON dead_letter_events (event_id) WHERE event_id IS NOT NULL;
CREATE INDEX idx_dead_letter_failed_at ON dead_letter_events (failed_at DESC);
