CREATE TABLE IF NOT EXISTS governance.batch_log (
    ingest_id               TEXT PRIMARY KEY,
    ingest_timestamp        TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    status                  TEXT NOT NULL,
    error_message           TEXT,
    
    -- Step 2 lineage fields
    file_count              INTEGER,
    files_success           INTEGER,
    files_error             INTEGER,
    batch_started_at_utc    TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    batch_completed_at_utc  TIMESTAMP WITHOUT TIME ZONE,
    
    -- Optional association to source_registry if desired
    source_id               TEXT,
    CONSTRAINT fk_batch_source
        FOREIGN KEY (source_id)
        REFERENCES governance.source_registry(source_id)
);
