
CREATE TABLE IF NOT EXISTS raw.ingest_file_log (
    ingest_id           VARCHAR(100) NOT NULL,
    source_id           VARCHAR(50) NOT NULL,
    file_name           TEXT NOT NULL,
    file_size_bytes     BIGINT NOT NULL,
    sha256_checksum     VARCHAR(128) NOT NULL,
    row_count_raw       BIGINT NOT NULL,
    ingested_at_utc     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- optional future fields:
    archived_path       TEXT,
    archived_flag       BOOLEAN DEFAULT FALSE,

    -- constraints
    CONSTRAINT ingest_file_log_pk PRIMARY KEY (ingest_id, file_name)
);
