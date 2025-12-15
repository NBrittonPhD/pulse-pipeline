-- =============================================================================
-- TABLE: governance.structure_qc_table
-- =============================================================================
-- Purpose:
--   Stores all schema validation issues detected during Step 3 × Cluster 3.
--   One row per issue (table-level or variable-level).
--   Supports schema versioning, severity classification, and governance audit.
--
-- Key Features:
--   • Ingest-scoped validation (ingest_id references batch_log)
--   • schema_version tracks which expected schema was used
--   • qc_issue_id uniquely identifies each issue
--   • lake_table_name / lake_variable_name anchor issues at table/column level
--   • expected_value / observed_value capture structural mismatch info
--   • expected_schema_hash / observed_schema_hash support reproducibility
--   • severity ("critical", "warning", "info") guides downstream decisioning
--   • issue_code + issue_group support analytics and dashboards
--   • check_context identifies where the issue was detected
--
-- Dependencies:
--   • governance.batch_log must exist (ingest_id FK)
--   • Expected schema dictionary maintained in reference schema
--
-- Governance:
--   • Append-only table. Rows must never be updated or deleted.
--   • Used by QC reports, metadata audits, and governance documentation.
-- =============================================================================

CREATE TABLE IF NOT EXISTS governance.structure_qc_table (
    
    -- Unique identifier for the QC issue
    qc_issue_id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Foreign key linking this QC issue to the ingest run
    ingest_id                TEXT NOT NULL,
    source_id                TEXT,
    source_type              TEXT,

    -- Schema version used to validate this ingest
    schema_version           TEXT,

    -- Table and variable where issue occurred
    lake_table_name          TEXT NOT NULL,
    lake_variable_name       TEXT NULL,

    -- Issue metadata
    issue_code               TEXT NOT NULL,
    issue_type               TEXT NOT NULL,
    issue_group              TEXT NOT NULL,        -- e.g., 'structural', 'dtype', 'completeness'
    severity                 TEXT NOT NULL CHECK (severity IN ('critical','warning','info')),
    is_blocking              BOOLEAN NOT NULL,

    -- Human-readable description of the issue
    issue_message            TEXT NOT NULL,

    -- Expected vs. observed values
    expected_value           TEXT,
    observed_value           TEXT,

    -- Schema hashes for reproducibility and audits
    expected_schema_hash     TEXT,
    observed_schema_hash     TEXT,

    -- Execution metadata
    check_context            TEXT,                 -- e.g., 'raw_table_level', 'variable_level'
    check_run_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by               TEXT NOT NULL DEFAULT 'schema_validation_engine',

    -- Optional free-text notes for human remediation
    notes                    TEXT,

    -- -------------------------------------------------------------------------
    -- Foreign keys
    -- -------------------------------------------------------------------------
    CONSTRAINT fk_qc_ingest
        FOREIGN KEY (ingest_id)
            REFERENCES governance.batch_log (ingest_id)
            ON DELETE CASCADE

);

-- -----------------------------------------------------------------------------
-- Indexes
-- -----------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_qc_ingest_id
    ON governance.structure_qc_table (ingest_id);

CREATE INDEX IF NOT EXISTS idx_qc_lake_table_name
    ON governance.structure_qc_table (lake_table_name);

CREATE INDEX IF NOT EXISTS idx_qc_issue_severity
    ON governance.structure_qc_table (severity);

CREATE INDEX IF NOT EXISTS idx_qc_issue_code
    ON governance.structure_qc_table (issue_code);

