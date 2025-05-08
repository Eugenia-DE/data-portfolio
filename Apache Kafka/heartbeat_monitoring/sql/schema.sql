CREATE TABLE IF NOT EXISTS heart_rate_records (
    customer_id INTEGER NOT NULL,
    heart_rate INTEGER NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    PRIMARY KEY(customer_id, timestamp)
);


CREATE INDEX idx_timestamp ON heart_rate_records(timestamp);

