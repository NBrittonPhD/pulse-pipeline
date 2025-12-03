-- =============================================================================
-- Create AUDIT_LOG table
-- Records all governance-relevant events in the PULSE pipeline
-- =============================================================================

CREATE TABLE IF NOT EXISTS governance.audit_log (

    -- Unique audit event identifier
    audit_id        VARCHAR(100) PRIMARY KEY,

    -- Optional ingest identifier (NULL -> permitted)
    ingest_id       VARCHAR(100),

    -- Human-readable action descriptor 
    -- (e.g., "source_registration|success|table|governance.source_registry")
    action          TEXT NOT NULL,

    -- JSON details (payload, metadata, event_type, object_name, etc.)
    details         JSONB NOT NULL,

    -- User executing the action
    executed_by     VARCHAR(100) NOT NULL DEFAULT SESSION_USER,

    -- Timestamp recorded by DB
    executed_at_utc TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Optional helpful index for JSON search performance
-- CREATE INDEX IF NOT EXISTS audit_log_details_gin_idx
--   ON governance.audit_log USING GIN (details);