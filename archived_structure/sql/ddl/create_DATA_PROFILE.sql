
CREATE TABLE DATA_PROFILE (
    profile_id         VARCHAR(50) PRIMARY KEY,
    ingest_id          VARCHAR(50) NOT NULL,
    lake_table_name    VARCHAR(100),
    lake_variable_name VARCHAR(100),
    metric_name        VARCHAR(100),
    metric_value       DOUBLE PRECISION,
    profile_timestamp_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
