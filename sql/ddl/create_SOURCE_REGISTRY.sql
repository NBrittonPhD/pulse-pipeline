
CREATE TABLE SOURCE_REGISTRY (
    source_id          VARCHAR(50) PRIMARY KEY,
    source_name        VARCHAR(200),
    source_description TEXT,
    steward_name       VARCHAR(200),
    owner_name         VARCHAR(200),
    retention_policy   TEXT,
    ingest_expectations TEXT,
    is_active          BOOLEAN DEFAULT TRUE,
    registered_at_utc  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
