-- =============================================================================
-- create_DATA_PROFILE_ISSUE.sql
-- =============================================================================
-- Purpose:      Create governance.data_profile_issue table for data quality
--               issues detected during profiling.
--
-- Schema:       governance
-- Grain:        One row per issue detected per variable (or table-level)
--
-- Dependencies: governance schema must exist
--               governance.batch_log must exist (FK target)
--
-- Author:       Noel
-- Last Updated: 2026-01-30
-- =============================================================================

CREATE TABLE IF NOT EXISTS governance.data_profile_issue (
    issue_id            SERIAL PRIMARY KEY,
    ingest_id           TEXT NOT NULL,
    schema_name         TEXT NOT NULL,
    table_name          TEXT NOT NULL,
    variable_name       TEXT,
    issue_type          TEXT NOT NULL,
    severity            TEXT NOT NULL CHECK (severity IN ('critical', 'warning', 'info')),
    description         TEXT,
    value               NUMERIC,
    recommendation      TEXT,

    CONSTRAINT fk_issue_ingest
        FOREIGN KEY (ingest_id)
        REFERENCES governance.batch_log (ingest_id)
        ON DELETE CASCADE
);

-- =============================================================================
-- INDEXES
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_data_profile_issue_ingest_id
    ON governance.data_profile_issue (ingest_id);

CREATE INDEX IF NOT EXISTS idx_data_profile_issue_severity
    ON governance.data_profile_issue (severity);

CREATE INDEX IF NOT EXISTS idx_data_profile_issue_table_var
    ON governance.data_profile_issue (table_name, variable_name);

-- =============================================================================
-- COMMENTS
-- =============================================================================

COMMENT ON TABLE governance.data_profile_issue IS
'Data quality issues detected during profiling. Severity: critical (identifier missing), warning (high missingness), info (moderate missingness, constant value, high cardinality). Written by Step 5: profile_data().';

COMMENT ON COLUMN governance.data_profile_issue.variable_name IS
'NULL for table-level issues; populated for variable-level issues.';

COMMENT ON COLUMN governance.data_profile_issue.value IS
'Numeric metric associated with the issue (e.g., missingness percentage).';
