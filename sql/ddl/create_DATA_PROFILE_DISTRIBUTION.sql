-- =============================================================================
-- create_DATA_PROFILE_DISTRIBUTION.sql
-- =============================================================================
-- Purpose:      Create governance.data_profile_distribution table for
--               numeric statistics and categorical frequency distributions.
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

CREATE TABLE IF NOT EXISTS governance.data_profile_distribution (
    distribution_id     SERIAL PRIMARY KEY,
    ingest_id           TEXT NOT NULL,
    schema_name         TEXT NOT NULL,
    table_name          TEXT NOT NULL,
    variable_name       TEXT NOT NULL,
    distribution_type   TEXT,
    stat_min            NUMERIC,
    stat_max            NUMERIC,
    stat_mean           NUMERIC,
    stat_median         NUMERIC,
    stat_sd             NUMERIC,
    stat_q25            NUMERIC,
    stat_q75            NUMERIC,
    stat_iqr            NUMERIC,
    top_values_json     TEXT,
    mode_value          TEXT,
    mode_count          INTEGER,
    mode_pct            NUMERIC(5,2),

    CONSTRAINT fk_dist_ingest
        FOREIGN KEY (ingest_id)
        REFERENCES governance.batch_log (ingest_id)
        ON DELETE CASCADE
);

-- =============================================================================
-- INDEXES
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_data_profile_dist_ingest_id
    ON governance.data_profile_distribution (ingest_id);

CREATE INDEX IF NOT EXISTS idx_data_profile_dist_table_var
    ON governance.data_profile_distribution (table_name, variable_name);

-- =============================================================================
-- COMMENTS
-- =============================================================================

COMMENT ON TABLE governance.data_profile_distribution IS
'Distribution statistics per variable. Numeric columns get min/max/mean/median/sd/quartiles. Categorical columns get top-N frequency as JSON. Written by Step 5: profile_data().';

COMMENT ON COLUMN governance.data_profile_distribution.distribution_type IS
'Either numeric or categorical.';

COMMENT ON COLUMN governance.data_profile_distribution.top_values_json IS
'JSON array of {value, count, pct} objects for categorical columns. NULL for numeric.';
