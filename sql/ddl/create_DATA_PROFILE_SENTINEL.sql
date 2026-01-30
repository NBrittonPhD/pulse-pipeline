-- =============================================================================
-- create_DATA_PROFILE_SENTINEL.sql
-- =============================================================================
-- Purpose:      Create governance.data_profile_sentinel table for detected
--               sentinel/placeholder values per variable.
--
-- Schema:       governance
-- Grain:        One row per (ingest_id, schema_name, table_name, variable_name,
--               sentinel_value)
--
-- Dependencies: governance schema must exist
--               governance.batch_log must exist (FK target)
--
-- Author:       Noel
-- Last Updated: 2026-01-30
-- =============================================================================

CREATE TABLE IF NOT EXISTS governance.data_profile_sentinel (
    sentinel_id         SERIAL PRIMARY KEY,
    ingest_id           TEXT NOT NULL,
    schema_name         TEXT NOT NULL,
    table_name          TEXT NOT NULL,
    variable_name       TEXT NOT NULL,
    sentinel_value      TEXT NOT NULL,
    sentinel_count      INTEGER,
    sentinel_pct        NUMERIC(5,2),
    detection_method    TEXT,
    confidence          TEXT,

    CONSTRAINT fk_sentinel_ingest
        FOREIGN KEY (ingest_id)
        REFERENCES governance.batch_log (ingest_id)
        ON DELETE CASCADE
);

-- =============================================================================
-- INDEXES
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_data_profile_sentinel_ingest_id
    ON governance.data_profile_sentinel (ingest_id);

CREATE INDEX IF NOT EXISTS idx_data_profile_sentinel_table_var
    ON governance.data_profile_sentinel (table_name, variable_name);

-- =============================================================================
-- COMMENTS
-- =============================================================================

COMMENT ON TABLE governance.data_profile_sentinel IS
'Detected sentinel/placeholder values per variable. Detection via config-based matching (high confidence) or frequency analysis (medium confidence). Written by Step 5: profile_data().';

COMMENT ON COLUMN governance.data_profile_sentinel.detection_method IS
'How the sentinel was detected: config_list or frequency_analysis.';

COMMENT ON COLUMN governance.data_profile_sentinel.confidence IS
'Detection confidence: high (config match) or medium (frequency pattern).';
