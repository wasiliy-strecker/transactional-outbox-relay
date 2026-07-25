CREATE TABLE purchase_order (
    id UUID PRIMARY KEY,
    customer_id VARCHAR(100) NOT NULL,
    total_amount NUMERIC(14, 2) NOT NULL CHECK (total_amount > 0),
    currency CHAR(3) NOT NULL,
    status VARCHAR(32) NOT NULL,
    version BIGINT NOT NULL CHECK (version > 0),
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX purchase_order_customer_idx
    ON purchase_order (customer_id, created_at DESC);
