-- =============================================================================
-- create_TRANSFORM_LOG.sql
-- =============================================================================
-- Purpose:      Create governance.transform_log table for auditing all
--               harmonization transformations during Step 6.
--
-- Schema:       governance
-- Grain:        One row per (source_table → target_table) operation per ingest
--
-- Dependencies: governance schema must exist
--               governance.batch_log must exist (FK target for ingest_id)
--
-- Author:       Noel
-- Last Updated: 2026-02-04
-- =============================================================================

CREATE TABLE IF NOT EXISTS governance.transform_log (
    -- Primary key
    transform_id             SERIAL PRIMARY KEY,

    -- Ingest tracking
    ingest_id                TEXT NOT NULL,

    -- Source
    source_schema            TEXT NOT NULL,       -- always 'staging'
    source_table             TEXT NOT NULL,
    source_row_count         INTEGER,

    -- Target
    target_schema            TEXT NOT NULL,       -- always 'validated'
    target_table             TEXT NOT NULL,
    target_row_count         INTEGER,

    -- Operation details
    operation_type           TEXT NOT NULL
        CHECK (operation_type IN ('insert', 'append', 'upsert', 'replace')),
    columns_mapped           INTEGER,

    -- Status
    status                   TEXT DEFAULT 'success'
        CHECK (status IN ('success', 'partial', 'failed')),
    error_message            TEXT,

    -- Timing
    started_at               TIMESTAMP,
    completed_at             TIMESTAMP,
    duration_seconds         NUMERIC,

    -- Constraints
    CONSTRAINT fk_transform_ingest
        FOREIGN KEY (ingest_id)
        REFERENCES governance.batch_log (ingest_id)
);

-- =============================================================================
-- INDEXES
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_transform_ingest
    ON governance.transform_log (ingest_id);

CREATE INDEX IF NOT EXISTS idx_transform_target
    ON governance.transform_log (target_schema, target_table);

CREATE INDEX IF NOT EXISTS idx_transform_status
    ON governance.transform_log (status);

-- =============================================================================
-- COMMENTS
-- =============================================================================

COMMENT ON TABLE governance.transform_log IS
'Audit trail of all harmonization transformations. One row per source_table → target_table operation per ingest. Populated by harmonize_table() during Step 6.';

COMMENT ON COLUMN governance.transform_log.ingest_id IS
'Batch identifier linking to governance.batch_log for lineage.';

COMMENT ON COLUMN governance.transform_log.source_schema IS
'Source schema (always staging for Step 6).';

COMMENT ON COLUMN governance.transform_log.source_table IS
'Staging table that was read.';

COMMENT ON COLUMN governance.transform_log.target_table IS
'Validated table that was written to.';

COMMENT ON COLUMN governance.transform_log.operation_type IS
'Type of write operation: insert (initial load), append, upsert, or replace.';

COMMENT ON COLUMN governance.transform_log.columns_mapped IS
'Number of columns mapped from source to target in this operation.';

COMMENT ON COLUMN governance.transform_log.status IS
'Outcome: success, partial (some columns missing), or failed.';

COMMENT ON COLUMN governance.transform_log.duration_seconds IS
'Wall-clock duration of this transformation in seconds.';
