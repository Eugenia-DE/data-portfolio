CREATE TABLE heart_rate (
    id SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    heart_rate INTEGER NOT NULL CHECK (heart_rate >= 20 AND heart_rate <= 220),
    is_anomaly BOOLEAN DEFAULT FALSE,
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_timestamp ON heart_rate (timestamp);
CREATE INDEX idx_customer_id ON heart_rate (customer_id);

