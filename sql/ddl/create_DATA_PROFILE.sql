-- =============================================================================
-- create_DATA_PROFILE.sql
-- =============================================================================
-- Purpose:      Create governance.data_profile table for variable-level
--               missingness and type profiling results.
--
-- Schema:       governance
-- Grain:        One row per (ingest_id, schema_name, table_name, variable_name)
--
-- Dependencies: governance schema must exist
--               governance.batch_log must exist (FK target)
--
-- Author:       Noel
-- Last Updated: 2026-01-30
-- =============================================================================

CREATE TABLE IF NOT EXISTS governance.data_profile (
    profile_id          SERIAL PRIMARY KEY,
    ingest_id           TEXT NOT NULL,
    schema_name         TEXT NOT NULL,
    table_name          TEXT NOT NULL,
    variable_name       TEXT NOT NULL,
    inferred_type       TEXT,
    total_count         INTEGER,
    valid_count         INTEGER,
    na_count            INTEGER,
    empty_count         INTEGER,
    whitespace_count    INTEGER,
    sentinel_count      INTEGER,
    na_pct              NUMERIC(5,2),
    empty_pct           NUMERIC(5,2),
    whitespace_pct      NUMERIC(5,2),
    sentinel_pct        NUMERIC(5,2),
    total_missing_count INTEGER,
    total_missing_pct   NUMERIC(5,2),
    valid_pct           NUMERIC(5,2),
    unique_count        INTEGER,
    unique_pct          NUMERIC(5,2),
    profiled_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_data_profile_ingest
        FOREIGN KEY (ingest_id)
        REFERENCES governance.batch_log (ingest_id)
        ON DELETE CASCADE
);

-- =============================================================================
-- INDEXES
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_data_profile_ingest_id
    ON governance.data_profile (ingest_id);

CREATE INDEX IF NOT EXISTS idx_data_profile_table_name
    ON governance.data_profile (table_name);

CREATE INDEX IF NOT EXISTS idx_data_profile_inferred_type
    ON governance.data_profile (inferred_type);

-- =============================================================================
-- COMMENTS
-- =============================================================================

COMMENT ON TABLE governance.data_profile IS
'Variable-level missingness and type profiling results. One row per variable per table per ingest batch. Written by Step 5: profile_data().';

COMMENT ON COLUMN governance.data_profile.inferred_type IS
'Inferred column type: numeric, categorical, date, or identifier.';

COMMENT ON COLUMN governance.data_profile.sentinel_count IS
'Count of values matching known sentinel/placeholder patterns (e.g., 999, UNKNOWN).';

COMMENT ON COLUMN governance.data_profile.total_missing_count IS
'Sum of na_count + empty_count + whitespace_count + sentinel_count.';
