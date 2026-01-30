-- =============================================================================
-- create_DATA_PROFILE_SUMMARY.sql
-- =============================================================================
-- Purpose:      Create governance.data_profile_summary table for per-table
--               quality summaries and scores.
--
-- Schema:       governance
-- Grain:        One row per (ingest_id, schema_name, table_name)
--
-- Dependencies: governance schema must exist
--               governance.batch_log must exist (FK target)
--
-- Author:       Noel
-- Last Updated: 2026-01-30
-- =============================================================================

CREATE TABLE IF NOT EXISTS governance.data_profile_summary (
    summary_id              SERIAL PRIMARY KEY,
    ingest_id               TEXT NOT NULL,
    schema_name             TEXT NOT NULL,
    table_name              TEXT NOT NULL,
    row_count               INTEGER,
    variable_count          INTEGER,
    avg_valid_pct           NUMERIC(5,2),
    min_valid_pct           NUMERIC(5,2),
    max_missing_pct         NUMERIC(5,2),
    critical_issue_count    INTEGER DEFAULT 0,
    warning_issue_count     INTEGER DEFAULT 0,
    info_issue_count        INTEGER DEFAULT 0,
    quality_score           TEXT,
    worst_variable          TEXT,
    worst_variable_missing_pct NUMERIC(5,2),

    CONSTRAINT fk_summary_ingest
        FOREIGN KEY (ingest_id)
        REFERENCES governance.batch_log (ingest_id)
        ON DELETE CASCADE
);

-- =============================================================================
-- INDEXES
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_data_profile_summary_ingest_id
    ON governance.data_profile_summary (ingest_id);

CREATE INDEX IF NOT EXISTS idx_data_profile_summary_quality
    ON governance.data_profile_summary (quality_score);

-- =============================================================================
-- COMMENTS
-- =============================================================================

COMMENT ON TABLE governance.data_profile_summary IS
'Per-table quality summary with aggregated metrics and quality score. Scores: Excellent, Good, Fair, Needs Review. Written by Step 5: profile_data().';

COMMENT ON COLUMN governance.data_profile_summary.quality_score IS
'Overall table quality: Excellent (<=5% missing, 0 critical), Good (<=10%, <=2), Fair (<=20%, <=5), Needs Review (otherwise).';

COMMENT ON COLUMN governance.data_profile_summary.worst_variable IS
'Variable name with the highest total_missing_pct in this table.';
