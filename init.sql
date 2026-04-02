CREATE TABLE IF NOT EXISTS maintenance_logs (
    id SERIAL PRIMARY KEY,
    machine_id VARCHAR(50) NOT NULL,
    rpm INTEGER NOT NULL,
    bearing_temperature INTEGER NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    anomaly_level VARCHAR(20) DEFAULT 'NORMAL',
    alert_message TEXT,
    INDEX idx_machine_id (machine_id),
    INDEX idx_timestamp (timestamp),
    INDEX idx_anomaly_level (anomaly_level)
);
