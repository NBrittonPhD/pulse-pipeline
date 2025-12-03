-- =============================================================================
-- Create SOURCE_REGISTRY table
-- Tracks all onboarded data sources for the PULSE pipeline
-- =============================================================================

CREATE TABLE IF NOT EXISTS governance.source_registry (
    
    -- Core identifiers
    source_id               VARCHAR(50) PRIMARY KEY,
    source_name             VARCHAR(200) NOT NULL,

    -- Classification + system metadata
    system_type             VARCHAR(50) NOT NULL,
    update_frequency        VARCHAR(50) NOT NULL,
    data_owner              VARCHAR(200) NOT NULL,
    ingest_method           VARCHAR(50) NOT NULL,
    expected_schema_version VARCHAR(50) NOT NULL,
    retention_policy        TEXT,
    pii_classification      VARCHAR(50) NOT NULL,

    -- Activity flag
    active                  BOOLEAN NOT NULL DEFAULT TRUE,

    -- Governance timestamps
    created_at_utc          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_modified_utc       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Who created the entry (defaults to SESSION_USER)
    created_by              VARCHAR(100) NOT NULL DEFAULT SESSION_USER
);

-- =============================================================================
-- Optional: Trigger to auto-update last_modified_utc on UPDATE
-- (Your workflow already created this via R as needed.)
-- =============================================================================

-- CREATE OR REPLACE FUNCTION governance.trg_update_last_modified_source_registry()
-- RETURNS TRIGGER AS $$
-- BEGIN
--     NEW.last_modified_utc = CURRENT_TIMESTAMP;
--     RETURN NEW;
-- END;
-- $$ LANGUAGE plpgsql;
--
-- CREATE TRIGGER update_last_modified_source_registry
-- BEFORE UPDATE ON governance.source_registry
-- FOR EACH ROW
-- EXECUTE FUNCTION governance.trg_update_last_modified_source_registry();