CREATE INDEX IF NOT EXISTS idx_ingest_file_log_ingest_id
    ON governance.ingest_file_log (ingest_id);