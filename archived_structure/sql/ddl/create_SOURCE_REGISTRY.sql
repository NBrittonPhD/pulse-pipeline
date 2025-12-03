-- ===========================
-- SOURCE_REGISTRY TABLE
-- ===========================

CREATE TABLE source_registry (
    source_id               VARCHAR(20) PRIMARY KEY,
    source_name             VARCHAR(100) NOT NULL,
    system_type             VARCHAR(20) NOT NULL CHECK (
                                system_type IN ('CSV','XLSX','SQL','API','FHIR','Other')
                             ),
    update_frequency        VARCHAR(20) NOT NULL CHECK (
                                update_frequency IN ('ad-hoc','daily','weekly','monthly','quarterly')
                             ),
    data_owner              VARCHAR(200) NOT NULL,
    ingest_method           VARCHAR(20) NOT NULL CHECK (
                                ingest_method IN ('push','pull','api_sync','manual_upload')
                             ),
    expected_schema_version VARCHAR(20) NOT NULL CHECK (
                                expected_schema_version ~ '^[0-9]+\\.[0-9]+\\.[0-9]+$'
                             ),
    retention_policy        TEXT,
    pii_classification      VARCHAR(20) NOT NULL CHECK (
                                pii_classification IN ('PHI','Limited','De-identified','Aggregate')
                             ),
    active                  BOOLEAN NOT NULL DEFAULT TRUE,

    -- timestamps
    created_at_utc          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_modified_utc       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- hybrid approach:
    -- DB session user is default, but your R function can override it.
    created_by              VARCHAR(100) NOT NULL DEFAULT SESSION_USER
);

-- =====================================================
-- TRIGGER: Auto-update last_modified_utc on any UPDATE
-- =====================================================

CREATE OR REPLACE FUNCTION trg_update_last_modified_source_registry()
RETURNS TRIGGER AS $$
BEGIN
    NEW.last_modified_utc = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_last_modified_source_registry
BEFORE UPDATE ON source_registry
FOR EACH ROW
EXECUTE FUNCTION trg_update_last_modified_source_registry();
