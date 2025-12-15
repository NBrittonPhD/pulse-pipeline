-- =============================================================================
-- PULSE Pipeline
-- Step 2 × Cluster 2 — Ingest File Lineage Log
-- =============================================================================
-- This table captures one row per file involved in a raw ingestion.
-- Primary key is a surrogate identity (ingest_file_id) to simplify updates
-- and ensure uniqueness even if file names repeat across ingests.
-- =============================================================================

CREATE TABLE IF NOT EXISTS governance.ingest_file_log (

    -- Unique surrogate key for each file log entry
    ingest_file_id     BIGSERIAL PRIMARY KEY,

    -- Foreign Key: ingestion batch identifier
    ingest_id          TEXT NOT NULL
                        REFERENCES governance.batch_log(ingest_id)
                        ON DELETE CASCADE,

    -- File identity
    file_name          TEXT NOT NULL,
    file_path          TEXT,

    -- Destination table
    lake_table_name    TEXT,

    -- File metadata
    file_size_bytes    BIGINT,
    row_count          BIGINT,
    checksum           TEXT,

    -- File-level load status
    load_status        TEXT NOT NULL
                        CHECK (load_status IN ('pending', 'success', 'error', 'skipped')),

    -- Timestamps
    logged_at_utc      TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    completed_at_utc   TIMESTAMP WITHOUT TIME ZONE
);
